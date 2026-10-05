#!/usr/bin/env python3
"""Flask server for the line-by-line Python execution observer.

Runs trusted local Python code in a dedicated subprocess (tracer.py) and
exposes a small JSON API:

  POST /api/run        {code}                       -> {run_id}
  POST /api/step       {run_id, pause_seq}          -> 200 | 409
  POST /api/continue   {run_id, pause_seq}          -> 200 | 409
  POST /api/terminate  {run_id}                     -> 200 | 409
  GET  /api/state                                   -> full run snapshot

Only one unfinished run is allowed at a time. Step/continue are accepted
at most once per pause; duplicate or stale requests are rejected with 409
and do not advance execution.

Concurrency model
-----------------
A single condition variable (COND) serialises run startup, termination and
event reading.  Each Run carries a child-process lifecycle:

  starting  -> status active ("running"), proc may not be assigned yet
  active    -> proc assigned, reader thread consumes events
  terminal  -> finished | exception | terminated (sticky, never goes back)
  reaped    -> the child process has been waited on (no zombie, no survivor)

Guarantees:

* terminate returns success only after the child is dead and reaped; a
  terminate that arrives while Popen is still in flight is remembered and
  the starter kills the freshly created child itself, so an unfinished
  start can never leave a process behind.
* a new run is not admitted while a previous child is still alive or being
  reaped; the starter waits for reaping (briefly) rather than racing it.
* late events (a pause already buffered in the pipe when the child was
  killed) are discarded once the run is terminal or no longer current, so
  they can never flip terminated/finished back to an active state.
"""

import json
import os
import subprocess
import sys
import threading
import uuid

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

MAX_LINES = 80
MAX_CODE_BYTES = 20_000
MAX_STDOUT_CHARS = 100_000

# Max time /api/run waits for an old child to be reaped, and /api/terminate
# waits for the reader thread to reap the child it just killed.
REAP_WAIT_TIMEOUT = 10.0

COND = threading.Condition()  # guards CURRENT and every Run lifecycle field
CURRENT = None  # the single current run (active or most recently finished)

ACTIVE_STATES = ("running", "paused")
TERMINAL_STATES = ("finished", "exception", "terminated")

TRACER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracer.py")


class Run:
    def __init__(self, code):
        self.run_id = uuid.uuid4().hex
        self.lines = code.splitlines()
        self.status = "running"  # running | paused | finished | exception | terminated
        self.pause_seq = 0       # incremented on every pause event
        self.pause_consumed = False  # one accepted step/continue per pause
        self.current_line = None
        self.locals = {}
        self.stdout = ""
        self.stdout_truncated = False
        self.exception = None
        # Child process lifecycle, all guarded by COND.
        self.proc = None             # subprocess.Popen handle once assigned
        self.proc_started = False    # Popen has returned (or definitively failed)
        self.reaped = False          # child has been waited on
        self.terminate_requested = False  # terminate arrived during startup

    def snapshot(self):
        return {
            "run_id": self.run_id,
            "status": self.status,
            "pause_seq": self.pause_seq,
            "current_line": self.current_line,
            "locals": self.locals,
            "stdout": self.stdout,
            "stdout_truncated": self.stdout_truncated,
            "exception": self.exception,
            "lines": self.lines,
        }


def _apply_event(run, event):
    """Apply one tracer event to the run. Called with COND held.

    Events are accepted only while this run is still the current, active
    run: anything arriving after a terminal transition (kill, natural end,
    exception) or after a newer run took over is discarded, so late events
    can never resurrect a finished/terminated run back into an active state.
    """
    if run is not CURRENT or run.status not in ACTIVE_STATES:
        return
    kind = event.get("type")
    if kind == "pause":
        run.pause_seq += 1
        run.pause_consumed = False
        run.status = "paused"
        run.current_line = event.get("line")
        run.locals = event.get("locals") or {}
    elif kind == "stdout":
        run.stdout += event.get("data", "")
        if len(run.stdout) > MAX_STDOUT_CHARS:
            run.stdout = run.stdout[-MAX_STDOUT_CHARS:]
            run.stdout_truncated = True
    elif kind == "exception":
        run.exception = event.get("exc") or "unknown exception"
        run.status = "exception"
    elif kind == "end":
        # Natural end only wins while active; a kill-driven terminal state
        # (terminated) stays sticky.
        if run.status in ACTIVE_STATES:
            run.status = "finished"


def reader_thread(run):
    """Consumes events from the tracer subprocess and reaps it at EOF.

    Reaping (proc.wait) happens here exactly once. terminate() kills the
    child and waits on run.reaped instead of waiting on the handle itself,
    so the child is never reaped concurrently from two threads.
    """
    proc = run.proc
    try:
        for raw in proc.stdout:
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            with COND:
                _apply_event(run, event)
    finally:
        try:
            proc.wait()  # reap the child process (normally the only wait)
        except ChildProcessError:
            pass  # terminate's fallback reaped it first
        finally:
            # Close the pipes Popen handed us (terminate closes stdin; the
            # read side is closed here; stderr is DEVNULL -> None).
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass
        with COND:
            run.reaped = True
            # Only a child that died on its own can still look active; a
            # terminate() call has already pinned the status to terminated.
            if run.status in ACTIVE_STATES:
                run.status = "finished"
            COND.notify_all()


def _kill_proc(proc):
    """Kill the tracer child and close its stdin. Safe to call once."""
    try:
        proc.kill()
    except OSError:
        pass
    try:
        proc.stdin.close()
    except OSError:
        pass


@app.get("/")
def index():
    return render_template("index.html", max_lines=MAX_LINES)


@app.post("/api/run")
def api_run():
    global CURRENT
    data = request.get_json(force=True, silent=True) or {}
    code = data.get("code")
    if not isinstance(code, str) or not code.strip():
        return jsonify({"error": "代码不能为空"}), 400
    if len(code.encode("utf-8", "ignore")) > MAX_CODE_BYTES:
        return jsonify({"error": "代码体积过大"}), 400
    line_count = len(code.splitlines())
    if line_count > MAX_LINES:
        return jsonify({"error": f"代码最多 {MAX_LINES} 行（当前 {line_count} 行）"}), 400

    with COND:
        # An active run (including one still starting up) conflicts. A
        # terminal run whose child is not reaped yet must finish reaping
        # before the next run is admitted, so the old process can neither
        # survive alongside the new one nor be reaped while it is current.
        old = CURRENT
        if old is not None and old.status in ACTIVE_STATES:
            return jsonify({"error": "已有未结束的运行，请先终止或等待其结束"}), 409
        if old is not None and not old.reaped:
            if not _wait_reaped(old):
                return jsonify({"error": "上一个运行的子进程尚未回收，请稍后重试"}), 409
            # While we waited another starter may have won the slot
            # (CURRENT never returns to None after the first run).
            if CURRENT is not old and CURRENT.status in ACTIVE_STATES:
                return jsonify({"error": "已有未结束的运行，请先终止或等待其结束"}), 409

        run = Run(code)
        CURRENT = run

    # Popen runs without the lock so a concurrent terminate (or another
    # failing system call) is never blocked behind process creation.
    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", TRACER_PATH],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
    except OSError:
        with COND:
            run.proc_started = True
            if run.status in ACTIVE_STATES:
                run.status = "exception"
                run.exception = "运行子进程启动失败"
            run.reaped = True  # no child was created
            COND.notify_all()
        return jsonify({"run_id": run.run_id})

    with COND:
        run.proc = proc
        run.proc_started = True
        if run.terminate_requested:
            # terminate() raced us and won: never start the reader or feed
            # code; kill and reap the child right here so that, by the time
            # terminate() returns, no process belonging to this run exists.
            _kill_proc(proc)
            proc.wait()
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass
            run.reaped = True
            run.status = "terminated"
            COND.notify_all()
            return jsonify({"run_id": run.run_id})
        threading.Thread(target=reader_thread, args=(run,), daemon=True).start()

    try:
        proc.stdin.write(json.dumps({"code": code}) + "\n")
        proc.stdin.flush()
    except (BrokenPipeError, OSError):
        with COND:
            if run.status in ACTIVE_STATES:
                run.status = "exception"
                run.exception = "运行子进程启动失败"
    return jsonify({"run_id": run.run_id})


def _wait_reaped(run, timeout=REAP_WAIT_TIMEOUT):
    """Wait until run.reaped. Must be called while holding/able to wait on
    COND. Returns True if reaped within the timeout."""
    COND.wait_for(lambda: run.reaped, timeout=timeout)
    return run.reaped


def _step_or_continue(command):
    data = request.get_json(force=True, silent=True) or {}
    run_id = data.get("run_id")
    pause_seq = data.get("pause_seq")
    with COND:
        run = CURRENT
        if run is None or run.run_id != run_id:
            return jsonify({"error": "运行不存在或已过期"}), 409
        if run.status != "paused":
            return jsonify({"error": "当前不在暂停状态，请求被拒绝"}), 409
        if pause_seq != run.pause_seq or run.pause_consumed:
            return jsonify({"error": "重复或过期的请求，已拒绝且未推进"}), 409
        run.pause_consumed = True
        run.status = "running"
        proc = run.proc
    try:
        proc.stdin.write(json.dumps({"cmd": command}) + "\n")
        proc.stdin.flush()
    except (BrokenPipeError, OSError):
        with COND:
            if run.status in ACTIVE_STATES:
                run.status = "finished"
        return jsonify({"error": "子进程已退出"}), 409
    return jsonify({"ok": True, "pause_seq": pause_seq})


@app.post("/api/step")
def api_step():
    return _step_or_continue("step")


@app.post("/api/continue")
def api_continue():
    return _step_or_continue("continue")


@app.post("/api/terminate")
def api_terminate():
    data = request.get_json(force=True, silent=True) or {}
    run_id = data.get("run_id")
    with COND:
        run = CURRENT
        if run is None or run.run_id != run_id:
            return jsonify({"error": "运行不存在或已过期"}), 409
        if run.status not in ACTIVE_STATES:
            # Terminal (finished/exception/terminated): stale request.
            return jsonify({"error": "运行不存在或已过期"}), 409
        # Terminal state is sticky and set before killing: any event the
        # reader still drains from the pipe is now a late event and is
        # discarded by _apply_event.
        run.status = "terminated"
        run.terminate_requested = True
        if not run.proc_started:
            # Startup is still inside Popen; the starter performs the
            # kill/reap itself as soon as the handle exists, and wakes us.
            COND.wait_for(lambda: run.proc_started or run.reaped)
        proc = run.proc  # set by the starter; None only if Popen failed
        needs_reap = not run.reaped

    if proc is not None:
        # Kill outside the lock; the reader performs the normal wait() and
        # flips run.reaped. During startup the starter did the reap itself.
        _kill_proc(proc)
    if needs_reap:
        with COND:
            if not _wait_reaped(run):
                # Reader failed to reap in time (should not happen): reap
                # here so the success guarantee still holds.
                COND.release()
                if proc is not None:
                    try:
                        proc.wait()
                    except ChildProcessError:
                        pass
                COND.acquire()
                run.reaped = True
                run.status = "terminated"
                COND.notify_all()
            status = run.status
    else:
        status = "terminated"
    return jsonify({"ok": True, "status": status})


@app.get("/api/state")
def api_state():
    with COND:
        if CURRENT is None:
            return jsonify({"status": "idle", "run_id": None})
        return jsonify(CURRENT.snapshot())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)

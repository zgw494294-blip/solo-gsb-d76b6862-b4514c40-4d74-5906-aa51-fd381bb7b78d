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

Lifecycle coordination (startup-time termination race)
-----------------------------------------------------
A run is registered (CURRENT) before its child process exists, and the
actual subprocess.Popen call cannot be performed under the global lock
(it would deadlock against a terminate that must wait for that very
spawn). Instead two primitives coordinate the window:

  LOCK        guards every visible run attribute (status, proc, events).
  STARTUP_COND a separate condition used only to wait for "the child has
              been fully spawned (or definitively failed to)".

If /api/terminate arrives while the child does not exist yet, it marks
the run terminated (terminate_requested) and waits on STARTUP_COND. When
Popen finishes, /api/run either hands the just-created child over to that
terminate (which kills and reaps it) or, if no terminate is pending,
publishes the run normally. In both cases /api/terminate returns success
only after the child is dead and reaped; an unfinished startup can never
leave a live or unreaped orphan behind.

  * /api/run refuses while CURRENT is not fully reaped (a child still
    alive or not yet wait()ed), so an immediate rerun cannot start while
    the previous child is outstanding.
  * Late events arriving after termination are discarded and cannot flip
    the run back to paused/finished.
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

# Guards all visible run state (status/proc/events/reaped).
LOCK = threading.RLock()
# Separate condition for the spawn window: terminate waits here for a
# not-yet-attached child without blocking the spawning thread.
STARTUP_COND = threading.Condition()
CURRENT = None  # the single current run (active or most recently finished)

# Internal statuses. "starting" only exists while the child is being
# spawned and is reported as "running" in snapshots.
ACTIVE_STATES = ("starting", "running", "paused")

TRACER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracer.py")


class Run:
    def __init__(self, code):
        self.run_id = uuid.uuid4().hex
        self.lines = code.splitlines()
        self.status = "starting"  # starting | running | paused | finished | exception | terminated
        self.pause_seq = 0       # incremented on every pause event
        self.pause_consumed = False  # one accepted step/continue per pause
        self.current_line = None
        self.locals = {}
        self.stdout = ""
        self.stdout_truncated = False
        self.exception = None
        self.proc = None
        self.reaped = False      # child has exited AND been wait()ed
        # spawn bookkeeping (guarded by STARTUP_COND's lock)
        self.spawn_done = False       # Popen finished (success or failure)
        self.terminate_requested = False  # terminate arrived during spawn

    def snapshot(self):
        # "starting" is an internal spawn-lock detail; clients see running.
        status = "running" if self.status == "starting" else self.status
        return {
            "run_id": self.run_id,
            "status": status,
            "pause_seq": self.pause_seq,
            "current_line": self.current_line,
            "locals": self.locals,
            "stdout": self.stdout,
            "stdout_truncated": self.stdout_truncated,
            "exception": self.exception,
            "lines": self.lines,
        }


def reader_thread(run):
    """Consumes events from the tracer subprocess and reaps it at EOF.

    Events are only applied while the run is still active: anything that
    arrives after termination (a late pause/stdout/end) is discarded so it
    can never resurrect the run's status. This thread is the single place
    that waits on the child; /api/terminate blocks on run.reaped.
    """
    proc = run.proc
    try:
        for raw in proc.stdout:
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            with LOCK:
                # Gate every event on liveness: terminated (or otherwise
                # finished) runs never accept late events.
                if run.status not in ACTIVE_STATES:
                    continue
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
                    if run.status in ACTIVE_STATES:
                        run.status = "finished"
    finally:
        # Reap the child (no LOCK held while waiting). reaped is published
        # exactly once, after the zombie is gone.
        try:
            proc.wait()
        except OSError:
            pass
        with LOCK:
            if run.status in ACTIVE_STATES:
                run.status = "finished"
            run.reaped = True
        with STARTUP_COND:
            STARTUP_COND.notify_all()


def _kill_and_wait_for_reap(run, proc, timeout=5):
    """Kill proc and block until the reader has reaped it."""
    try:
        proc.kill()
    except OSError:
        pass
    with STARTUP_COND:
        STARTUP_COND.wait_for(lambda: run.reaped, timeout=timeout)
    if not run.reaped:
        # Defensive fallback if the reader is delayed: wait() is safe to
        # call concurrently with the reader.
        try:
            proc.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, OSError):
            pass
        with STARTUP_COND:
            STARTUP_COND.wait_for(lambda: run.reaped, timeout=2)


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

    with LOCK:
        # Reject while the previous child is alive or not yet reaped.
        if CURRENT is not None and not CURRENT.reaped:
            return jsonify({"error": "已有未结束的运行，请先终止或等待其结束"}), 409
        run = Run(code)
        CURRENT = run

    # Popen is performed WITHOUT LOCK: holding it would deadlock against a
    # terminate waiting for this spawn to attach the child.
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
        with LOCK:
            run.status = "exception"
            run.exception = "运行子进程启动失败"
            run.reaped = True
        with STARTUP_COND:
            run.spawn_done = True
            STARTUP_COND.notify_all()
        return jsonify({"run_id": run.run_id})

    # Publish the child atomically with the terminate-intent decision, all
    # under STARTUP_COND: a terminate either arrives before this point (its
    # terminate_requested flag is observed here and the spawn thread kills +
    # reaps the child) or after it (it is guaranteed to see run.proc and does
    # the kill itself). There is no in-between where terminate sees neither
    # a pending flag nor an attached proc.
    with STARTUP_COND:
        abort = run.terminate_requested
        run.spawn_done = True
        with LOCK:
            run.proc = proc
            if abort:
                run.status = "terminated"
        STARTUP_COND.notify_all()

    if abort:
        # Terminate arrived during spawn: no reader was started, so reap the
        # just-created child here (kill, drain EOF, close pipes) and publish
        # reaped; the waiting /api/terminate then returns success.
        try:
            proc.kill()
        except OSError:
            pass
        try:
            proc.communicate(timeout=5)
        except (subprocess.TimeoutExpired, OSError):
            try:
                proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                pass
        with LOCK:
            run.reaped = True
        with STARTUP_COND:
            STARTUP_COND.notify_all()
        return jsonify({"run_id": run.run_id})

    with LOCK:
        threading.Thread(target=reader_thread, args=(run,), daemon=True).start()
        write_failed = False
        try:
            proc.stdin.write(json.dumps({"code": code}) + "\n")
            proc.stdin.flush()
        except (BrokenPipeError, OSError):
            # Child died immediately; the reader drains EOF and reaps.
            write_failed = True
        if write_failed:
            if run.status in ACTIVE_STATES:
                run.status = "exception"
            run.exception = "运行子进程启动失败"
        elif run.status == "starting":
            run.status = "running"
    return jsonify({"run_id": run.run_id})


def _step_or_continue(command):
    data = request.get_json(force=True, silent=True) or {}
    run_id = data.get("run_id")
    pause_seq = data.get("pause_seq")
    with LOCK:
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
        with LOCK:
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

    with LOCK:
        run = CURRENT
        if run is None or run.run_id != run_id:
            return jsonify({"error": "运行不存在或已过期"}), 409
        if run.status in ACTIVE_STATES:
            run.status = "terminated"
        proc = run.proc

    if proc is None:
        # Child not attached yet (startup window). Record the intent and wait
        # for the spawn to finish; /api/run hands the child over and kills it
        # (or, on spawn failure, the run is already reaped).
        with STARTUP_COND:
            run.terminate_requested = True
            STARTUP_COND.wait_for(
                lambda: run.spawn_done or run.reaped, timeout=10
            )
        with LOCK:
            proc = run.proc
        if proc is not None:
            _kill_and_wait_for_reap(run, proc)
        # Spawn failure path: run is reaped already.
    else:
        if not run.reaped:
            _kill_and_wait_for_reap(run, proc)

    with LOCK:
        return jsonify({"ok": True, "status": run.status})


@app.get("/api/state")
def api_state():
    with LOCK:
        if CURRENT is None:
            return jsonify({"status": "idle", "run_id": None})
        return jsonify(CURRENT.snapshot())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)

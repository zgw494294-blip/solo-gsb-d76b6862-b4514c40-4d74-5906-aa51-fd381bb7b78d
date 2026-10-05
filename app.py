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

LOCK = threading.Lock()
CURRENT = None  # the single current run (active or most recently finished)

ACTIVE_STATES = ("running", "paused")

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
        self.proc = None

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


def reader_thread(run):
    """Consumes events from the tracer subprocess and reaps it at EOF."""
    proc = run.proc
    try:
        for raw in proc.stdout:
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            with LOCK:
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
        proc.wait()  # reap the child process
        with LOCK:
            if run.status in ACTIVE_STATES:
                run.status = "finished"


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
        if CURRENT is not None and CURRENT.status in ACTIVE_STATES:
            return jsonify({"error": "已有未结束的运行，请先终止或等待其结束"}), 409
        run = Run(code)
        CURRENT = run

    proc = subprocess.Popen(
        [sys.executable, "-u", TRACER_PATH],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    run.proc = proc
    threading.Thread(target=reader_thread, args=(run,), daemon=True).start()
    try:
        proc.stdin.write(json.dumps({"code": code}) + "\n")
        proc.stdin.flush()
    except (BrokenPipeError, OSError):
        with LOCK:
            run.status = "exception"
            run.exception = "运行子进程启动失败"
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
    proc = None
    with LOCK:
        run = CURRENT
        if run is None or run.run_id != run_id:
            return jsonify({"error": "运行不存在或已过期"}), 409
        if run.status in ACTIVE_STATES:
            run.status = "terminated"
            proc = run.proc
    if proc is not None:
        try:
            proc.kill()
        except OSError:
            pass
        try:
            proc.wait(timeout=5)  # reap the child
        except subprocess.TimeoutExpired:
            pass
    return jsonify({"ok": True, "status": run.status})


@app.get("/api/state")
def api_state():
    with LOCK:
        if CURRENT is None:
            return jsonify({"status": "idle", "run_id": None})
        return jsonify(CURRENT.snapshot())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)

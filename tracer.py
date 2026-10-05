#!/usr/bin/env python3
"""Tracer subprocess: executes trusted user code under bdb and talks to the
parent Flask process over stdin (commands) / stdout (events), one JSON
object per line.

Protocol
--------
parent -> child:  {"code": "..."}          (first line, init)
parent -> child:  {"cmd": "step"|"continue"|"terminate"}
child  -> parent: {"type": "pause", "line": N, "locals": {...}}
child  -> parent: {"type": "stdout", "data": "..."}
child  -> parent: {"type": "exception", "exc": "..."}
child  -> parent: {"type": "end"}
"""

import bdb
import io
import json
import sys
import traceback

USER_FILENAME = "<user_code>"
MAX_REPR = 200

REAL_OUT = sys.stdout
REAL_IN = sys.stdin


def send(event):
    REAL_OUT.write(json.dumps(event, ensure_ascii=False) + "\n")
    REAL_OUT.flush()


class StdoutProxy(io.TextIOBase):
    """Redirects user prints into stdout events for the parent."""

    def write(self, s):
        if s:
            send({"type": "stdout", "data": s})
        return len(s)

    def flush(self):
        pass


def safe_repr(value):
    try:
        text = repr(value)
    except Exception:
        return "<unrepresentable>"
    if len(text) > MAX_REPR:
        text = text[:MAX_REPR] + "…"
    return text


def snapshot_locals(frame):
    result = {}
    for name, value in frame.f_locals.items():
        if name.startswith("__") and name.endswith("__"):
            continue
        result[name] = safe_repr(value)
    return result


def format_user_exc(exc):
    """Traceback limited to user-code frames plus the exception line."""
    tb = traceback.extract_tb(exc.__traceback__) if exc.__traceback__ else []
    user_frames = [f for f in tb if f.filename == USER_FILENAME]
    parts = []
    if user_frames:
        parts.extend(traceback.format_list(user_frames))
    parts.extend(traceback.format_exception_only(type(exc), exc))
    return "".join(parts)


class Tracer(bdb.Bdb):
    def user_line(self, frame):
        if frame.f_code.co_filename != USER_FILENAME:
            # Line inside stdlib etc.: keep stepping until back in user code.
            self.set_step()
            return
        send({
            "type": "pause",
            "line": frame.f_lineno,
            "locals": snapshot_locals(frame),
        })
        raw = REAL_IN.readline()
        if not raw:
            raise SystemExit  # parent went away
        cmd = json.loads(raw).get("cmd")
        if cmd == "step":
            self.set_step()
        elif cmd == "continue":
            self.set_continue()
        elif cmd == "terminate":
            raise SystemExit
        else:
            self.set_step()


def main():
    init = json.loads(REAL_IN.readline())
    code = init["code"]

    # Interactive input is not supported: stdin reads hit EOF immediately.
    sys.stdin = io.StringIO("")
    sys.stdout = StdoutProxy()
    sys.stderr = StdoutProxy()

    tracer = Tracer()
    tracer.set_step()  # stop at the first executable line
    namespace = {"__name__": "__main__"}
    try:
        compiled = compile(code, USER_FILENAME, "exec")
        tracer.runctx(compiled, namespace, namespace)
    except SystemExit:
        pass
    except BaseException as exc:  # noqa: BLE001 - report anything to the UI
        send({"type": "exception", "exc": format_user_exc(exc)})
    finally:
        send({"type": "end"})


if __name__ == "__main__":
    main()

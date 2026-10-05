#!/usr/bin/env python3
"""Controlled-interleave checks for the run/terminate/reader race fixes.

Run directly:  python race_check.py

Scenarios:
  1. terminate during startup (child Popen not finished yet), then an
     immediate rerun: terminate must still reap the first child, no two
     live children may coexist, and the stale run_id must be rejected.
  2. /api/run while the previous child is still alive/unreaped -> 409.
  3. late events after terminate (pause/stdout/end) must not flip the
     terminated status back; the child must be reaped.
  4. normal pause + step/continue dedupe rules (pause_seq) unchanged.
  5. infinite loop: continue then terminate reaps the child and a rerun
     is accepted.
"""

import json
import os
import threading
import time
import unittest

import app as appmod

TRACER_PATH = appmod.TRACER_PATH

SIMPLE_CODE = "x = 1\nprint('hi')\n"
PAUSE_CODE = "a = 1\nb = a + 2\nprint(b)\n"
LOOP_CODE = "while True:\n    pass\n"


def wait_for(predicate, timeout=5.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None and value is not False:
            return value
        time.sleep(interval)
    return predicate()


def reset_state():
    with appmod.LOCK:
        run = appmod.CURRENT
        appmod.CURRENT = None
    if run is not None and run.proc is not None and hasattr(run.proc, "poll"):
        try:
            if run.proc.poll() is None:
                run.proc.kill()
                run.proc.wait(timeout=5)
            for stream in (getattr(run.proc, "stdin", None),
                           getattr(run.proc, "stdout", None),
                           getattr(run.proc, "stderr", None)):
                if stream is not None and hasattr(stream, "close"):
                    stream.close()
        except Exception:
            pass


class ApiClient:
    def __init__(self):
        self.client = appmod.app.test_client()

    def run(self, code):
        return self.client.post("/api/run", json={"code": code})

    def terminate(self, run_id):
        return self.client.post("/api/terminate", json={"run_id": run_id})

    def step(self, run_id, pause_seq):
        return self.client.post("/api/step", json={"run_id": run_id, "pause_seq": pause_seq})

    def cont(self, run_id, pause_seq):
        return self.client.post("/api/continue", json={"run_id": run_id, "pause_seq": pause_seq})

    def state(self):
        return self.client.get("/api/state").get_json()


class GatedPopen:
    """Wraps subprocess.Popen with two controllable interleave points:

      * spawn_allowed gates entry into real Popen (CURRENT registered,
        child object not existing yet -- the original orphan window);
      * assign_allowed gates the time after Popen returns but while the
        reader/current-proc wiring is still in progress.
    """

    spawn_allowed = threading.Event()
    assign_allowed = threading.Event()
    entered = threading.Event()
    created = threading.Event()
    instances = []
    real_popen = appmod.subprocess.Popen

    def __init__(self, *args, **kwargs):
        type(self).entered.set()
        type(self).spawn_allowed.wait()  # controlled interleave: no timeout
        self._real = type(self).real_popen(*args, **kwargs)
        type(self).instances.append(self._real)
        type(self).created.set()
        type(self).assign_allowed.wait()

    def __getattr__(self, name):
        return getattr(self._real, name)

    @classmethod
    def install(cls):
        cls.spawn_allowed.clear()
        cls.assign_allowed.clear()
        cls.entered.clear()
        cls.created.clear()
        cls.instances = []
        appmod.subprocess.Popen = cls

    @classmethod
    def release_spawn(cls):
        cls.spawn_allowed.set()

    @classmethod
    def release_assign(cls):
        cls.assign_allowed.set()

    @classmethod
    def uninstall(cls):
        appmod.subprocess.Popen = cls.real_popen
        cls.spawn_allowed.set()
        cls.assign_allowed.set()


class StartupTerminateRaceTests(unittest.TestCase):
    def setUp(self):
        reset_state()
        self.api = ApiClient()

    def tearDown(self):
        GatedPopen.uninstall()
        reset_state()

    def test_terminate_during_startup_then_rerun(self):
        GatedPopen.install()

        started = {}

        def do_run():
            started["resp"] = self.api.run(SIMPLE_CODE)

        t = threading.Thread(target=do_run)
        t.start()

        # Window A: run registered, thread is blocked before real Popen
        # (holding COND), child process object does not exist yet.
        self.assertTrue(GatedPopen.entered.wait(5))
        run1 = wait_for(lambda: appmod.CURRENT)
        self.assertIsNotNone(run1)
        self.assertIsNone(run1.proc, "child must not exist yet at this interleave point")
        with appmod.LOCK:
            self.assertEqual(run1.status, "starting")

        # 1) A rerun during this exact spawn window blocks on COND; it is
        #    serialized behind the in-progress startup (and behind the
        #    terminate issued below). The "active run -> rerun 409" rule is
        #    covered separately in test_rerun_while_active below.

        # 2) Terminate during startup: sent in the no-child window. It must
        #    block until startup finishes the spawn and the child is reaped.
        term_result = {}

        def do_terminate():
            term_result["resp"] = self.api.terminate(run1.run_id)

        tt = threading.Thread(target=do_terminate)
        tt.start()
        time.sleep(0.2)
        self.assertNotIn("resp", term_result, "terminate must block while startup is in progress")

        GatedPopen.release_spawn()
        # Window B: real child now exists but wiring is still in progress.
        self.assertTrue(GatedPopen.created.wait(5))
        time.sleep(0.1)
        self.assertNotIn("resp", term_result)
        GatedPopen.release_assign()

        t.join(5)
        tt.join(5)

        self.assertEqual(started["resp"].status_code, 200)
        resp = term_result["resp"]
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "terminated")

        # First child: dead + reaped, no orphan.
        proc1 = GatedPopen.instances[0]
        self.assertTrue(wait_for(lambda: proc1.poll() is not None))
        with appmod.LOCK:
            self.assertTrue(run1.reaped)
            self.assertEqual(run1.status, "terminated")

        # 3) Immediate rerun succeeds and creates exactly one live child.
        resp2 = self.api.run(SIMPLE_CODE)
        self.assertEqual(resp2.status_code, 200)
        run2_id = resp2.get_json()["run_id"]
        self.assertNotEqual(run2_id, run1.run_id)
        run2 = wait_for(lambda: appmod.CURRENT if appmod.CURRENT.run_id == run2_id else None)
        self.assertIsNotNone(run2)
        self.assertIsNotNone(run2.proc)

        # Old run_id can no longer drive the new process: stale terminate 409.
        stale = self.api.terminate(run1.run_id)
        self.assertEqual(stale.status_code, 409)
        self.assertIsNone(run2.proc.poll(), "new child must still be alive after stale terminate")

        # Exactly one live child exists (the old one is reaped, not alive).
        live = [p for p in GatedPopen.instances if p.poll() is None]
        self.assertEqual(len(live), 1, f"expected one live child, found {len(live)}")
        self.assertEqual(live[0].pid, run2.proc.pid)

        # Clean up the second run properly.
        ok = self.api.terminate(run2_id)
        self.assertEqual(ok.status_code, 200)
        self.assertTrue(wait_for(lambda: run2.reaped))
        self.assertIsNotNone(run2.proc.poll())  # reaped -> poll returns code

    def test_rerun_while_active(self):
        # No gating here: a normal live run rejects concurrent /api/run.
        r = self.api.run(LOOP_CODE)
        self.assertEqual(r.status_code, 200)
        run_id = r.get_json()["run_id"]
        st = wait_for(lambda: s if (s := self.api.state())["status"] == "paused" else None)
        self.assertIsNotNone(st)
        self.api.cont(run_id, st["pause_seq"])
        time.sleep(0.1)

        conflict = self.api.run(SIMPLE_CODE)
        self.assertEqual(conflict.status_code, 409)
        with appmod.LOCK:
            self.assertEqual(appmod.CURRENT.run_id, run_id)

        self.assertEqual(self.api.terminate(run_id).status_code, 200)
        # Only after reap is a new run accepted.
        accepted = self.api.run(SIMPLE_CODE)
        self.assertEqual(accepted.status_code, 200)
        self.api.terminate(accepted.get_json()["run_id"])


class LateEventTests(unittest.TestCase):
    """Late events arriving after terminate must not resurrect status."""

    def setUp(self):
        reset_state()

    def tearDown(self):
        reset_state()

    def test_late_events_keep_terminated(self):
        class FakeProc:
            """Pipe is empty until kill(), after which it delivers the late
            pause/stdout/end events that a dying child could still flush."""

            def __init__(self):
                self.stdout = self
                self.killed = threading.Event()
                self.iteration_done = threading.Event()

            def __iter__(self):
                self.killed.wait(5)  # block until terminate kills the child
                yield json.dumps({"type": "stdout", "data": "late output"})
                yield json.dumps({"type": "pause", "line": 2, "locals": {"x": "'z'"}})
                yield json.dumps({"type": "end"})
                self.iteration_done.set()

            def kill(self):
                self.killed.set()

            def poll(self):
                return -9 if self.iteration_done.is_set() else None

            def wait(self, timeout=None):
                self.iteration_done.wait(5)
                return -9

        with appmod.LOCK:
            run = appmod.Run(PAUSE_CODE)
            run.status = "running"
            appmod.CURRENT = run
        fake = FakeProc()
        run.proc = fake
        threading.Thread(target=appmod.reader_thread, args=(run,), daemon=True).start()

        api = ApiClient()
        resp = api.terminate(run.run_id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "terminated")

        with appmod.LOCK:
            self.assertTrue(run.reaped, "terminate must return only after reaping")
            self.assertEqual(run.status, "terminated", "late events must not flip status")
            self.assertEqual(run.stdout, "", "late stdout event must be discarded")
            self.assertEqual(run.pause_seq, 0, "late pause event must be discarded")
            self.assertIsNone(run.current_line)


class NormalPauseTests(unittest.TestCase):
    def setUp(self):
        reset_state()
        self.api = ApiClient()

    def tearDown(self):
        reset_state()

    def test_pause_step_dedupe_and_continue(self):
        r = self.api.run(PAUSE_CODE)
        self.assertEqual(r.status_code, 200)
        run_id = r.get_json()["run_id"]

        st = wait_for(lambda: s if (s := self.api.state())["status"] == "paused" else None)
        self.assertIsNotNone(st)
        self.assertEqual(st["current_line"], 1)
        first_seq = st["pause_seq"]
        self.assertEqual(first_seq, 1)

        # Duplicate/stale step: same pause_seq twice -> second one 409.
        ok = self.api.step(run_id, first_seq)
        self.assertEqual(ok.status_code, 200)
        dup = self.api.step(run_id, first_seq)
        self.assertEqual(dup.status_code, 409)
        stale = self.api.cont(run_id, first_seq - 1)
        self.assertEqual(stale.status_code, 409)

        # Next pause, then continue to finish.
        st = wait_for(lambda: s if (s := self.api.state())["status"] == "paused" and s["pause_seq"] == 2 else None)
        self.assertIsNotNone(st)
        self.assertEqual(st["current_line"], 2)
        ok = self.api.cont(run_id, st["pause_seq"])
        self.assertEqual(ok.status_code, 200)
        st = wait_for(lambda: s if (s := self.api.state())["status"] == "finished" else None)
        self.assertIsNotNone(st)
        self.assertIn("3", st["stdout"])

        run = appmod.CURRENT
        self.assertTrue(wait_for(lambda: run.reaped))
        # After finish+reap, a rerun is allowed.
        again = self.api.run(SIMPLE_CODE)
        self.assertEqual(again.status_code, 200)
        self.api.terminate(again.get_json()["run_id"])


class InfiniteLoopTests(unittest.TestCase):
    def setUp(self):
        reset_state()
        self.api = ApiClient()

    def tearDown(self):
        reset_state()

    def test_terminate_infinite_loop_then_rerun(self):
        r = self.api.run(LOOP_CODE)
        self.assertEqual(r.status_code, 200)
        run_id = r.get_json()["run_id"]
        st = wait_for(lambda: s if (s := self.api.state())["status"] == "paused" else None)
        self.assertIsNotNone(st)

        ok = self.api.cont(run_id, st["pause_seq"])
        self.assertEqual(ok.status_code, 200)
        time.sleep(0.2)  # let the loop spin

        run = appmod.CURRENT
        self.assertIsNone(run.proc.poll(), "loop child should still be alive")
        resp = self.api.terminate(run_id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "terminated")
        self.assertTrue(wait_for(lambda: run.reaped))
        self.assertIsNotNone(run.proc.poll(), "loop child must be reaped after terminate")

        # Repeating terminate on the same terminated run stays 200 and cannot
        # touch anything (no child left); a bogus id is still a 409.
        again_term = self.api.terminate(run_id)
        self.assertEqual(again_term.status_code, 200)
        bogus = self.api.terminate("deadbeef")
        self.assertEqual(bogus.status_code, 409)
        # A new run is accepted immediately after reaping.
        again = self.api.run(PAUSE_CODE)
        self.assertEqual(again.status_code, 200)
        new_id = again.get_json()["run_id"]
        self.assertNotEqual(new_id, run_id)
        st = wait_for(lambda: s if (s := self.api.state())["run_id"] == new_id and s["status"] == "paused" else None)
        self.assertIsNotNone(st)
        # The old id cannot drive the new run either.
        old = self.api.terminate(run_id)
        self.assertEqual(old.status_code, 409)
        self.api.terminate(new_id)


class StressTests(unittest.TestCase):
    """Repeated run+terminate must never accumulate live/orphan children."""

    def setUp(self):
        reset_state()
        self.api = ApiClient()

    def tearDown(self):
        reset_state()

    def test_repeated_run_and_terminate(self):
        codes = [PAUSE_CODE, SIMPLE_CODE, LOOP_CODE]
        for i in range(20):
            code = codes[i % len(codes)]
            r = self.api.run(code)
            self.assertEqual(r.status_code, 200, i)
            run_id = r.get_json()["run_id"]
            run = wait_for(lambda: appmod.CURRENT if appmod.CURRENT.run_id == run_id else None)
            self.assertIsNotNone(run)
            # Terminate both at the first pause and (for loops) mid-run.
            if code == LOOP_CODE and i % 2 == 0:
                st = wait_for(lambda: s if (s := self.api.state())["run_id"] == run_id and s["status"] == "paused" else None)
                self.assertIsNotNone(st)
                self.api.cont(run_id, st["pause_seq"])
                time.sleep(0.02)
            elif code == PAUSE_CODE:
                wait_for(lambda: s if (s := self.api.state())["run_id"] == run_id and s["status"] == "paused" else None)
            term = self.api.terminate(run_id)
            self.assertEqual(term.status_code, 200, i)
            self.assertTrue(wait_for(lambda: run.reaped), f"run {i} not reaped")
            self.assertIsNotNone(run.proc.poll(), f"run {i} child still alive")

        # Across all iterations only the last CURRENT remains, and it is reaped.
        with appmod.LOCK:
            self.assertTrue(appmod.CURRENT.reaped)


class StartupFuzzTests(unittest.TestCase):
    """Fire terminate at random points across the whole startup window;
    no child may survive and CURRENT must always end reaped."""

    def setUp(self):
        reset_state()
        self.api = ApiClient()

    def tearDown(self):
        reset_state()

    def test_terminate_fuzz_during_startup(self):
        import random
        rng = random.Random(1234)
        for i in range(40):
            box = {}

            def do_run():
                box["r"] = self.api.run(LOOP_CODE)

            t = threading.Thread(target=do_run)
            t.start()
            delay = rng.uniform(0.0, 0.03)
            time.sleep(delay)
            # Run id is assigned synchronously before Popen returns, but guard
            # for the case the thread has not registered yet.
            run_id = None
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with appmod.LOCK:
                    if appmod.CURRENT is not None:
                        run_id = appmod.CURRENT.run_id
                        break
                time.sleep(0.001)
            self.assertIsNotNone(run_id, f"iter {i}: run never registered")
            term = self.api.terminate(run_id)
            self.assertEqual(term.status_code, 200, i)
            t.join(5)
            run = appmod.CURRENT
            self.assertTrue(wait_for(lambda: run.reaped), f"iter {i} not reaped")
            with appmod.LOCK:
                self.assertEqual(run.status, "terminated", f"iter {i}: {run.status}")
                self.assertIsNotNone(run.proc.poll(), f"iter {i}: child alive")
            # Rerun must be accepted right away and old id be stale.
            again = self.api.run(PAUSE_CODE)
            self.assertEqual(again.status_code, 200, i)
            new_id = again.get_json()["run_id"]
            self.assertEqual(self.api.terminate(run_id).status_code, 409, i)
            st = wait_for(lambda: s if (s := self.api.state())["run_id"] == new_id else None)
            self.assertIsNotNone(st)
            self.assertEqual(self.api.terminate(new_id).status_code, 200)
            self.assertTrue(wait_for(lambda: appmod.CURRENT.reaped))


if __name__ == "__main__":
    unittest.main(verbosity=2)

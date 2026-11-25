"""One test or more per invariant of the README (I1 to I5). Each one broke the
runner at commit 9ed3142."""
import os
import subprocess
import sys
import threading
import time

import pytest

from remake import credits
from remake import worker as W
from remake.provider import SimProvider
from remake.worker import cancel_job
from tests.conftest import ACCOUNT, charges, events, new_job, submissions
from tests.fakes import FakeFlow, make_worker
from tests.test_runner import ROOT, job, steps


def hold(conn, job_id):
    return conn.execute("select status from credit_holds where job_id=%s", (job_id,)).fetchone()["status"]


def in_thread(fn):
    box = {}
    t = threading.Thread(target=lambda: box.update(r=fn()), daemon=True)
    t.start()
    return t, box


# ------------------------------------------------------------------ I1

def test_a_submission_that_never_returns_is_bounded_and_sent_once(dsn, conn, monkeypatch):
    orig, hung = SimProvider.submit, []

    def first_send_hangs(self, key, step, spec):
        if step == "clip:02" and not hung:
            hung.append(key)
            time.sleep(3)  # an HTTP call that does not come back in time, then arrives late
        return orig(self, key, step, spec)
    monkeypatch.setattr(SimProvider, "submit", first_send_hangs)
    jid = new_job(conn)
    t, box = in_thread(lambda: make_worker(dsn, "w1", status_timeout_s=0.2).run_once())
    try:
        t.join(2.0)
        assert not t.is_alive(), "the worker is still waiting on a submission 2 s after a 0.2 s timeout"
        assert box["r"][1] == "succeeded"
        assert events(conn, jid, "provider_call_timeout")
        time.sleep(1.5)  # the hung first send reaches the provider now, with the same client key
        assert submissions(conn, jid)["clip:02"] == 1
    finally:
        t.join(10)
        time.sleep(1.5)


def test_a_job_never_stays_running_past_its_deadline(dsn, conn):
    jid = new_job(conn)
    w = make_worker(dsn, "w1", faults={"clip:02": ["pending_forever"]}, provider_deadline_s=4.0, max_attempts=1)
    w.job_deadline_s = 1.0
    t, box = in_thread(w.run_once)
    try:
        time.sleep(2.0)
        j = job(conn, jid)
        assert j["status"] == "failed", f"job still {j['status']} 1 s past its deadline"
        assert "deadline" in j["error"]
        assert hold(conn, jid) == "released" and charges(conn, jid) == []
    finally:
        t.join(10)


def test_a_job_past_its_deadline_is_failed_by_any_other_worker(dsn, conn, tmp_path):
    jid = new_job(conn)
    a = make_worker(dsn, "A", flow=FakeFlow(tmp_path, edit_s=4.0))
    a.job_deadline_s = 1.0
    t, box = in_thread(a.run_once)
    try:
        time.sleep(1.5)  # A is in its long edit, lease live, deadline passed
        assert make_worker(dsn, "B").run_once() is None  # nothing to run: B settled the overdue job instead
        j = job(conn, jid)
        assert j["status"] == "failed" and "deadline" in j["error"]
        assert hold(conn, jid) == "released"
        t.join(3)
        assert not t.is_alive() and box["r"][1] == "fenced"
        assert not (tmp_path / "out").exists()
    finally:
        t.join(10)


# ------------------------------------------------------------------ I2

def spawn_and_kill(dsn, job_id, point):
    return subprocess.run([sys.executable, "-m", "tests.fakes", dsn, str(job_id), point], cwd=ROOT,
                          env={**os.environ, "PYTHONPATH": str(ROOT)}, capture_output=True, text=True).returncode


@pytest.mark.parametrize("point", ["edit:ran", "clip:03:answered"])
def test_a_step_that_kills_its_worker_every_time_fails_the_job(dsn, conn, point):
    jid = new_job(conn)
    kills = 0
    for _ in range(6):
        if spawn_and_kill(dsn, jid, point) != -9:
            break  # this worker refused to pick the step up again
        kills += 1
        time.sleep(1.1)  # the dead worker's 1 s lease runs out
    j = job(conn, jid)
    assert j["status"] == "failed", f"after {kills} workers killed at {point}: {j['status']}"
    assert kills == 4
    assert "picked up by 4 workers" in j["error"]
    assert hold(conn, jid) == "released" and charges(conn, jid) == []


def test_a_submission_that_always_times_out_fails_after_max_attempts(dsn, conn, monkeypatch):
    orig = SimProvider.submit

    def always_late(self, key, step, spec):
        if step == "clip:02":
            time.sleep(1.0)
        return orig(self, key, step, spec)
    monkeypatch.setattr(SimProvider, "submit", always_late)
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", status_timeout_s=0.1, max_attempts=3).run_once()
    time.sleep(1.3)  # the late sends land now
    assert status == "failed"
    assert steps(conn, jid)["clip:02"]["attempts"] == 3
    assert "clip:02 failed after 3 attempts" in job(conn, jid)["error"]
    assert submissions(conn, jid).get("clip:02", 0) <= 1  # every send used the same client key
    assert hold(conn, jid) == "released"


# ------------------------------------------------------------------ I3

def test_a_cancel_during_the_final_edit_publishes_nothing(dsn, conn, tmp_path):
    jid = new_job(conn)
    answer = {}
    w = make_worker(dsn, "w1", flow=FakeFlow(tmp_path),
                    on={"edit:ran": lambda _: answer.update(a=cancel_job(conn, jid))})
    _, status = w.run_once()
    assert answer["a"] == "cancel_requested"
    assert status == "cancelled" and job(conn, jid)["status"] == "cancelled"
    assert not (tmp_path / "out" / f"film_{jid}.txt").exists()
    assert charges(conn, jid) == [] and hold(conn, jid) == "released"


# ------------------------------------------------------------------ I4

def test_a_frozen_heartbeat_stops_the_worker_before_anyone_takes_over(dsn, conn, tmp_path, monkeypatch):
    # a renewal whose database call never returns: no exception, no renewal
    monkeypatch.setattr(W.Worker, "_heartbeat", lambda self: time.sleep(30))
    jid = new_job(conn)
    a = make_worker(dsn, "A", flow=FakeFlow(tmp_path, edit_s=3.0))
    t0 = time.monotonic()
    t, box = in_thread(a.run_once)
    try:
        t.join(1.5)
        assert not t.is_alive(), "worker A is still working after its 1 s lease ran out"
        assert time.monotonic() - t0 < 1.0  # it stopped before its lease could be taken over
        assert box["r"][1] == "fenced"
        monkeypatch.undo()
        time.sleep(max(0.0, t0 + 1.2 - time.monotonic()))  # now the lease runs out
        _, status = make_worker(dsn, "B", flow=FakeFlow(tmp_path)).run_once()
        assert status == "succeeded"
        out, took = events(conn, jid, "fenced_out"), events(conn, jid, "lease_taken_over")
        assert out[0]["worker"] == "A" and out[0]["id"] < took[0]["id"]
        assert "renewed" in out[0]["detail"]["reason"]
        assert (tmp_path / "out" / f"film_{jid}.txt").read_text() == "rendered under fence 2"
    finally:
        t.join(10)


# ------------------------------------------------------------------ I5

BAD_PURCHASES = {
    "superscript two": {"credits": "²"},
    "arabic-indic digits": {"credits": "١٢"},
    "too large": {"credits": "99999999999"},
    "exponent": {"credits": "1e3"},
    "empty": {"credits": ""},
    "null": {"credits": None},
    "list": {"credits": ["5"]},
    "metadata as text": "credits account_id",
    "account as object": {"credits": "5", "account_id": {"id": ACCOUNT}},
}


@pytest.mark.parametrize("case", list(BAD_PURCHASES))
def test_a_malformed_credit_purchase_is_recorded_not_raised(dsn, conn, case):
    meta = BAD_PURCHASES[case]
    if isinstance(meta, dict):
        meta = {"account_id": ACCOUNT, **meta}
    ev = {"id": "evt_bad", "type": "checkout.session.completed",
          "data": {"object": {"id": "cs_bad", "payment_intent": "pi_bad", "payment_status": "paid", "metadata": meta}}}
    effect = credits.handle_stripe_event(conn, ev)
    assert effect.startswith("ignored"), effect
    assert conn.execute("select effect from stripe_events where event_id='evt_bad'").fetchone()["effect"] == effect
    assert credits.balance(conn, ACCOUNT)["balance"] == 500

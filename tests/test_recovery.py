"""Crash, freeze and cancel at every boundary of a step. Each test is a scenario
that broke an earlier version of the runner."""
import hashlib
import subprocess
import threading
import time

import pytest

from remake import credits, db, media
from remake.export import export_films
from remake.worker import cancel_job
from tests.conftest import ACCOUNT, charges, events, new_job, submissions
from tests.fakes import FakeFlow, make_worker
from tests.test_runner import CLIPS, job, kill_worker_at, steps


def calls(conn, job_id, step):
    return conn.execute("select * from provider_calls where job_id=%s and step=%s order by attempt",
                        (job_id, step)).fetchall()


def hold(conn, job_id):
    return conn.execute("select status from credit_holds where job_id=%s", (job_id,)).fetchone()["status"]


# ------------------------------------------------------------------ resuming a clip

def test_crash_between_the_provider_answer_and_the_step_record_sends_nothing_new(dsn, conn):
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:03:answered")  # the provider has finished clip 3; nothing recorded yet
    time.sleep(1.1)
    _, status = make_worker(dsn, "w2").run_once()
    assert status == "succeeded"
    assert submissions(conn, jid) == {c: 1 for c in CLIPS}
    assert [c["status"] for c in calls(conn, jid, "clip:03")] == ["done"]


def test_a_done_call_whose_step_was_not_recorded_is_recovered(dsn, conn):
    # the state an earlier version left when it died between its two writes
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:03:answered")
    conn.execute("update provider_calls set status='done' where job_id=%s and step='clip:03'", (jid,))
    time.sleep(1.1)
    _, status = make_worker(dsn, "w2").run_once()
    assert status == "succeeded"
    assert submissions(conn, jid)["clip:03"] == 1
    assert [e["step"] for e in events(conn, jid, "provider_result_recovered")] == ["clip:03"]


def test_crash_after_the_last_failed_attempt_sends_nothing_more(dsn, conn):
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:03:failed:a4", faults={"clip:03": ["fail"] * 4})
    assert submissions(conn, jid)["clip:03"] == 4
    time.sleep(1.1)
    _, status = make_worker(dsn, "w2").run_once()
    assert status == "failed"
    assert submissions(conn, jid)["clip:03"] == 4  # no fifth submission
    assert job(conn, jid)["error"].startswith("clip:03 failed after 4 attempts: provider failed clip:03: 503")
    assert hold(conn, jid) == "released"


def test_killed_between_writing_the_intent_and_sending_is_sent_once(dsn, conn):
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:03:intent")
    assert [c["status"] for c in calls(conn, jid, "clip:03")] == ["intent"]
    assert "clip:03" not in submissions(conn, jid)  # the provider never got it
    time.sleep(1.1)
    _, status = make_worker(dsn, "w2").run_once()
    assert status == "succeeded"  # not needs_review: the provider said it had no such request
    assert submissions(conn, jid)["clip:03"] == 1
    assert [e["step"] for e in events(conn, jid, "provider_request_absent")] == ["clip:03"]
    assert calls(conn, jid, "clip:03")[0]["client_key"] == f"job{jid}:clip:03:a1"  # same key, same attempt


def test_connection_dropped_before_the_provider_got_it_is_retried(dsn, conn):
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", faults={"clip:03": ["lost_before_accept"]}).run_once()
    assert status == "succeeded"
    assert submissions(conn, jid)["clip:03"] == 1
    assert [c["status"] for c in calls(conn, jid, "clip:03")] == ["lost", "done"]
    assert len(charges(conn, jid)) == 1


def test_frozen_before_sending_then_sending_late_creates_one_request(dsn, conn):
    jid = new_job(conn)
    frozen = make_worker(dsn, "frozen", stall_at="clip:03:send", stall_s=2.2)
    result = {}
    t = threading.Thread(target=lambda: result.update(r=frozen.run_once()))
    t.start()
    time.sleep(1.5)  # its lease (1 s) has run out while it waits to send
    _, status = make_worker(dsn, "rescuer").run_once()
    assert status == "succeeded"
    t.join()
    assert result["r"][1] == "fenced"
    assert submissions(conn, jid)["clip:03"] == 1  # the late send reused the client key
    assert len(charges(conn, jid)) == 1


# ------------------------------------------------------------------ one outcome

def test_cancel_that_arrives_just_before_success_wins_and_nothing_is_charged(dsn, conn):
    jid = new_job(conn)
    answer = {}
    w = make_worker(dsn, "w1", on={"before_succeed": lambda _: answer.update(a=cancel_job(conn, jid))})
    _, status = w.run_once()
    assert answer["a"] == "cancel_requested"
    assert status == "cancelled" and job(conn, jid)["status"] == "cancelled"
    assert charges(conn, jid) == []
    assert credits.balance(conn, ACCOUNT) == {"balance": 500, "held": 0, "available": 500}


def test_cancel_resolves_a_job_waiting_for_review_and_releases_its_credits(dsn, conn):
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", faults={"clip:03": ["lost_response"]}, lookup=False).run_once()
    assert status == "needs_review" and hold(conn, jid) == "held"
    assert cancel_job(conn, jid) == "cancelled"
    assert hold(conn, jid) == "released"
    assert credits.balance(conn, ACCOUNT)["available"] == 500
    assert [c["status"] for c in calls(conn, jid, "clip:03")] == ["abandoned"]
    assert make_worker(dsn, "w2").claim() is None


def test_cancel_of_a_job_whose_worker_died_settles_when_the_lease_runs_out(dsn, conn):
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:02")
    assert cancel_job(conn, jid) == "cancel_requested"  # the lease is still valid: maybe it is only slow
    assert hold(conn, jid) == "held"
    time.sleep(1.1)
    assert make_worker(dsn, "w2").run_once() is None  # nothing to run: the cancel was settled instead
    j = job(conn, jid)
    assert j["status"] == "cancelled" and j["fence"] == 2  # the dead worker's fence no longer writes
    assert hold(conn, jid) == "released" and charges(conn, jid) == []


def test_cancel_after_success_changes_nothing(dsn, conn):
    jid = new_job(conn)
    make_worker(dsn, "w1").run_once()
    assert cancel_job(conn, jid) == "succeeded"
    assert len(charges(conn, jid)) == 1 and hold(conn, jid) == "consumed"


# ------------------------------------------------------------------ nothing waits for ever

def test_provider_that_never_answers_hits_the_deadline_and_frees_the_credits(dsn, conn):
    jid = new_job(conn)
    t0 = time.monotonic()
    _, status = make_worker(dsn, "w1", faults={"clip:02": ["pending_forever"] * 2}, max_attempts=2,
                            provider_deadline_s=0.3).run_once()
    assert status == "failed" and time.monotonic() - t0 < 5
    assert "no answer from the provider 0.3 s after submission" in job(conn, jid)["error"]
    assert hold(conn, jid) == "released"
    states = conn.execute("select state from sim_provider.requests where step='clip:02'").fetchall()
    assert [s["state"] for s in states] == ["cancelled", "cancelled"]


def test_a_status_call_that_never_returns_is_bounded_by_the_clock(dsn, conn):
    jid = new_job(conn)
    t0 = time.monotonic()
    _, status = make_worker(dsn, "w1", faults={"clip:02": ["hang"]}, max_attempts=1, hang_s=4.0,
                            status_timeout_s=0.2, provider_deadline_s=0.5).run_once()
    assert status == "failed" and time.monotonic() - t0 < 3
    assert events(conn, jid, "provider_call_timeout")
    assert hold(conn, jid) == "released"


def test_a_frozen_subprocess_is_killed_and_the_job_fails_cleanly(dsn, conn):
    jid = new_job(conn)
    t0 = time.monotonic()
    _, status = make_worker(dsn, "w1", flow=FakeFlow(stuck="prep"), max_attempts=2).run_once()
    assert status == "failed" and time.monotonic() - t0 < 5
    assert job(conn, jid)["error"] == "prep failed after 2 attempts: sleep still running after 0.5 s; stopped"
    assert hold(conn, jid) == "released"
    assert subprocess.run(["pgrep", "-f", "sleep 31.4159"], capture_output=True).stdout == b""


def test_a_lost_heartbeat_stops_the_worker_before_anyone_takes_over(dsn, conn, tmp_path):
    jid = new_job(conn)
    a = make_worker(dsn, "A", flow=FakeFlow(tmp_path, edit_s=6.0))
    result = {}
    t = threading.Thread(target=lambda: result.update(r=a.run_once()))
    t.start()
    deadline = time.monotonic() + 5
    while not events(conn, jid, "step_started") or events(conn, jid, "step_started")[-1]["step"] != "edit":
        assert time.monotonic() < deadline
        time.sleep(0.02)
    killed = conn.execute("select pg_terminate_backend(pid) as ok from pg_stat_activity"
                          " where application_name='heartbeat:A'").fetchall()
    assert killed and killed[0]["ok"]
    t_kill = time.monotonic()
    t.join(5)
    assert result["r"][1] == "fenced"
    assert time.monotonic() - t_kill < 1.0  # stopped well before its 1 s lease could be taken over
    assert "lease lost" in a.stop_reason
    assert events(conn, jid, "fenced_out") == [] and job(conn, jid)["published"] is None
    time.sleep(1.1)
    _, status = make_worker(dsn, "B", flow=FakeFlow(tmp_path)).run_once()
    assert status == "succeeded"
    export_films(conn, tmp_path / "out")
    assert (tmp_path / "out" / f"film_{jid}.txt").read_text() == "rendered under fence 2"


# ------------------------------------------------------------------ files under the fence

def test_a_late_worker_can_only_leave_unpublished_files(dsn, conn, tmp_path):
    jid = new_job(conn)
    late = make_worker(dsn, "late", flow=FakeFlow(tmp_path), stall_at="edit:ran", stall_s=2.2)
    result = {}
    t = threading.Thread(target=lambda: result.update(r=late.run_once()))
    t.start()
    time.sleep(1.5)  # it rendered the film, then froze before recording it; its lease ran out
    _, status = make_worker(dsn, "rescuer", flow=FakeFlow(tmp_path)).run_once()
    assert status == "succeeded"
    t.join()
    assert result["r"][1] == "fenced"
    assert not (tmp_path / "out").exists()  # the workers never write to out/
    late_file, kept = tmp_path / f"job{jid}" / "f1" / "film.txt", tmp_path / f"job{jid}" / "f2" / "film.txt"
    assert late_file.exists()  # the late file stays in its own folder
    pub = job(conn, jid)["published"]
    assert [(p["name"], p["path"]) for p in pub] == [(f"film_{jid}.txt", str(kept))]
    assert pub[0]["sha256"] == hashlib.sha256(kept.read_bytes()).hexdigest()
    assert export_films(conn, tmp_path / "out") == [(jid, f"film_{jid}.txt", "exported")]
    assert export_films(conn, tmp_path / "out") == [(jid, f"film_{jid}.txt", "unchanged")]  # idempotent
    out = tmp_path / "out" / f"film_{jid}.txt"
    assert out.read_text() == "rendered under fence 2"  # the rescuer's film
    assert sorted(p.name for p in out.parent.iterdir()) == [out.name]  # no stray temporary file


def test_voice_cache_entries_are_written_whole_or_not_at_all(tmp_path):
    target = tmp_path / "abc.wav"

    def half_then_crash(tmp):
        tmp.write_bytes(b"RIFF half a file")
        raise OSError("disk full")
    with pytest.raises(OSError):
        media.atomic_write(target, half_then_crash)
    assert list(tmp_path.iterdir()) == []
    media.atomic_write(target, lambda tmp: tmp.write_bytes(b"whole"))
    assert target.read_bytes() == b"whole"
    assert media._cached(tmp_path, "abc") is None  # no .json marker yet: not a cache entry
    (tmp_path / "abc.json").write_text("[]")
    assert media._cached(tmp_path, "abc")


def test_an_aborted_subprocess_is_stopped_at_once():
    flag = threading.Event()
    threading.Timer(0.3, flag.set).start()
    t0 = time.monotonic()
    with pytest.raises(media.Aborted):
        media.run(["sleep", "31.4159"], timeout=30, abort=flag)
    assert time.monotonic() - t0 < 2

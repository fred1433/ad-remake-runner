"""Job runner: leases, fencing, retries with a ceiling, resumable steps.

A worker claims a job by taking a lease and bumping the job's fence number.
Every write it makes afterwards re-reads the job row under lock and checks that
the fence is still its own: a worker that stalled past its lease, while another
took the job over, can no longer change anything.
"""
import json
import os
import signal
import threading
import time
from contextlib import contextmanager

from . import credits, db
from .provider import ProviderRejected, ResponseLost


class Fenced(Exception):
    """Another worker holds this job now."""


class Transient(Exception):
    """Worth another attempt (provider down, provider failed the generation)."""


class StopJob(Exception):
    """The job needs a person; the worker leaves it alone."""


def log(conn, job_id, worker, kind, step=None, **detail):
    conn.execute("insert into events (job_id, worker, kind, step, detail) values (%s,%s,%s,%s,%s::jsonb)",
                 (job_id, worker, kind, step, json.dumps(detail)))


def create_job(conn, account_id, kind, params, step_names, cost):
    with conn.transaction():
        # Lock the account before the job row exists: the job's foreign key takes a
        # share lock on the account, and two creators upgrading it would deadlock.
        conn.execute("select id from accounts where id=%s for update", (account_id,))
        job_id = conn.execute("insert into jobs (account_id, kind, params) values (%s,%s,%s::jsonb) returning id",
                              (account_id, kind, json.dumps(params))).fetchone()["id"]
        credits.hold(conn, account_id, job_id, cost)
        for i, name in enumerate(step_names):
            conn.execute("insert into steps (job_id, name, seq) values (%s,%s,%s)", (job_id, name, i))
        log(conn, job_id, None, "job_created", credits_held=cost, steps=len(step_names))
    return job_id


def cancel_job(conn, job_id):
    with conn.transaction():
        j = conn.execute("select status from jobs where id=%s for update", (job_id,)).fetchone()
        if j["status"] == "queued":
            released = credits.release(conn, job_id)
            conn.execute("update jobs set status='cancelled', updated_at=clock_timestamp() where id=%s", (job_id,))
            log(conn, job_id, None, "job_cancelled", reservation_released=released)
            return "cancelled"
        if j["status"] == "running":
            conn.execute("update jobs set cancel_requested=true where id=%s", (job_id,))
            log(conn, job_id, None, "cancel_requested")
            return "cancel_requested"
        return j["status"]


class Worker:
    def __init__(self, dsn, name, impl, provider=None, lease_s=8.0, backoff_base=1.0, backoff_cap=8.0,
                 max_attempts=4, poll_s=0.25, provider_wait_s=1.5, crash_at=None, stall_at=None, stall_s=0.0):
        self.dsn, self.name, self.impl, self.provider = dsn, name, impl, provider
        self.lease_s, self.backoff_base, self.backoff_cap = lease_s, backoff_base, backoff_cap
        self.max_attempts, self.poll_s, self.provider_wait_s = max_attempts, poll_s, provider_wait_s
        self.crash_at, self.stall_at, self.stall_s = crash_at, stall_at, stall_s
        self.job = self.fence = None
        self._hb_stop = threading.Event()
        self._resumed = set()

    # ------------------------------------------------------------ claiming
    def claim(self, only_job=None):
        with db.connect(self.dsn) as c:
            row = c.execute(
                """with pick as (
                     select id, lease_owner as prev_owner, status as prev_status from jobs
                     where (status='queued' or (status='running' and lease_expires_at < clock_timestamp()))
                       and (%s::bigint is null or id=%s)
                     order by id for update skip locked limit 1)
                   update jobs j set status='running', lease_owner=%s, fence=j.fence+1,
                     lease_expires_at=clock_timestamp() + make_interval(secs => %s), updated_at=clock_timestamp()
                   from pick where j.id=pick.id
                   returning j.id, j.fence, pick.prev_owner, pick.prev_status""",
                (only_job, only_job, self.name, self.lease_s)).fetchone()
            if row:
                kind = "lease_taken_over" if row["prev_status"] == "running" else "claimed"
                log(c, row["id"], self.name, kind, fence=row["fence"], previous_owner=row["prev_owner"],
                    lease_s=self.lease_s)
        return row

    def run_once(self, only_job=None):
        row = self.claim(only_job)
        if not row:
            return None
        return row["id"], self.process(row["id"], row["fence"])

    # ------------------------------------------------------------ lease
    def _heartbeat(self):
        with db.connect(self.dsn) as c:
            while not self._hb_stop.wait(self.lease_s / 3):
                n = c.execute("update jobs set lease_expires_at=clock_timestamp() + make_interval(secs => %s)"
                              " where id=%s and fence=%s and status='running'",
                              (self.lease_s, self.job, self.fence)).rowcount
                if n == 0:
                    return

    @contextmanager
    def owned(self, c):
        with c.transaction():
            r = c.execute("select fence, status, cancel_requested from jobs where id=%s for update",
                          (self.job,)).fetchone()
            if r["fence"] != self.fence or r["status"] != "running":
                raise Fenced(f"job {self.job}: fence {self.fence} is stale (now {r['fence']}, {r['status']})")
            yield r

    def _checkpoint(self, point):
        if self.crash_at == point:
            os.kill(os.getpid(), signal.SIGKILL)
        if self.stall_at == point:
            self._hb_stop.set()
            time.sleep(self.stall_s)

    # ------------------------------------------------------------ main loop
    def process(self, job_id, fence):
        self.job, self.fence = job_id, fence
        self._hb_stop.clear()
        hb = threading.Thread(target=self._heartbeat, daemon=True)
        hb.start()
        try:
            with db.connect(self.dsn) as c:
                return self._loop(c)
        except Fenced as e:
            with db.connect(self.dsn) as c:
                log(c, self.job, self.name, "fenced_out", reason=str(e))
            return "fenced"
        finally:
            self._hb_stop.set()

    def _loop(self, c):
        while True:
            with self.owned(c) as j:
                if j["cancel_requested"]:
                    released = credits.release(c, self.job)
                    c.execute("update jobs set status='cancelled', lease_owner=null, updated_at=clock_timestamp()"
                              " where id=%s", (self.job,))
                    log(c, self.job, self.name, "job_cancelled", reservation_released=released)
                    return "cancelled"
                step = c.execute("select *, greatest(0, extract(epoch from next_attempt_at - clock_timestamp()))"
                                 " as wait_s from steps where job_id=%s and status<>'done' order by seq limit 1",
                                 (self.job,)).fetchone()
            if step is None:
                return self._succeed(c)
            if step["wait_s"] and float(step["wait_s"]) > 0:
                time.sleep(float(step["wait_s"]))
                continue
            try:
                out = self._run_clip(c, step) if step["name"].startswith("clip:") else self._run_plain(c, step)
            except Fenced:
                raise
            except StopJob:
                return "needs_review"
            except Exception as e:  # noqa: BLE001 - every failure is classified below
                if self._fail_attempt(c, step, e):
                    return "failed"
                continue
            if out is None:
                continue
            with self.owned(c):
                c.execute("update steps set status='done', output=%s::jsonb, next_attempt_at=null, last_error=null"
                          " where job_id=%s and name=%s", (json.dumps(out), self.job, step["name"]))
                n = c.execute("select attempts from steps where job_id=%s and name=%s",
                              (self.job, step["name"])).fetchone()["attempts"]
                log(c, self.job, self.name, "step_done", step["name"], attempt=n)
            self._checkpoint(step["name"])

    def _outputs(self, c):
        rows = c.execute("select name, output from steps where job_id=%s and status='done'", (self.job,)).fetchall()
        return {r["name"]: r["output"] for r in rows}

    def _job(self, c):
        return c.execute("select * from jobs where id=%s", (self.job,)).fetchone()

    def _run_plain(self, c, step):
        with self.owned(c):
            c.execute("update steps set attempts=attempts+1 where job_id=%s and name=%s", (self.job, step["name"]))
            log(c, self.job, self.name, "step_started", step["name"], attempt=step["attempts"] + 1)
        return self.impl.run(self._job(c), step["name"], self._outputs(c))

    def _run_clip(self, c, step):
        name = step["name"]
        call = c.execute("select * from provider_calls where job_id=%s and step=%s and status in ('intent','submitted')"
                         " order by attempt desc limit 1", (self.job, name)).fetchone()
        if call is None:
            with self.owned(c):
                attempt = c.execute("update steps set attempts=attempts+1, status='pending' where job_id=%s and name=%s"
                                    " returning attempts", (self.job, name)).fetchone()["attempts"]
                key = f"job{self.job}:{name}:a{attempt}"
                c.execute("insert into provider_calls (client_key, job_id, step, attempt, status)"
                          " values (%s,%s,%s,%s,'intent')", (key, self.job, name, attempt))
            spec = self.impl.clip_spec(self._job(c), name, self._outputs(c))
            rid = None
            try:
                rid = self.provider.submit(key, name, spec)
            except ProviderRejected as e:
                with self.owned(c):
                    c.execute("update provider_calls set status='rejected' where client_key=%s", (key,))
                raise Transient(f"provider refused the submission: {e}")
            except ResponseLost as e:
                with self.owned(c):
                    log(c, self.job, self.name, "provider_response_lost", name, client_key=key, error=str(e))
            if rid:
                with self.owned(c):
                    c.execute("update provider_calls set request_id=%s, status='submitted', updated_at=clock_timestamp()"
                              " where client_key=%s", (rid, key))
                    log(c, self.job, self.name, "provider_submitted", name, attempt=attempt, request_id=rid)
            self._checkpoint(f"{name}:submitted")
            call = c.execute("select * from provider_calls where client_key=%s", (key,)).fetchone()
        rid = call["request_id"]
        if rid is None:
            rid = self.provider.lookup(call["client_key"])
            with self.owned(c):
                if rid is None:
                    msg = (f"{name}: the provider may have accepted request {call['client_key']} but never answered,"
                           " and it cannot be looked up. Not resubmitted, to avoid paying for it twice."
                           " Credits stay held until someone checks the provider dashboard.")
                    c.execute("update provider_calls set status='unknown' where client_key=%s", (call["client_key"],))
                    c.execute("update steps set status='outcome_unknown', last_error=%s where job_id=%s and name=%s",
                              (msg, self.job, name))
                    c.execute("update jobs set status='needs_review', error=%s, lease_owner=null,"
                              " updated_at=clock_timestamp() where id=%s", (msg, self.job))
                    log(c, self.job, self.name, "outcome_unknown", name, client_key=call["client_key"])
            if rid is None:
                raise StopJob(msg)  # raised after the commit above
            with self.owned(c):
                c.execute("update provider_calls set request_id=%s, status='submitted' where client_key=%s",
                          (rid, call["client_key"]))
                log(c, self.job, self.name, "provider_request_found", name, request_id=rid, client_key=call["client_key"])
        else:
            sub = c.execute("select worker from events where job_id=%s and kind='provider_submitted'"
                            " and detail->>'request_id'=%s", (self.job, rid)).fetchone()
            if sub and sub["worker"] != self.name and rid not in self._resumed:
                self._resumed.add(rid)
                with self.owned(c):
                    log(c, self.job, self.name, "provider_polling_resumed", name, request_id=rid,
                        submitted_by=sub["worker"], note="request id found in the database: polled, not resubmitted")
        deadline = time.monotonic() + self.provider_wait_s
        while True:
            st = self.provider.status(rid)
            if st["state"] == "done":
                with self.owned(c):
                    c.execute("update provider_calls set status='done', updated_at=clock_timestamp() where request_id=%s", (rid,))
                return {"clip": st["result"], "request_id": rid}
            if st["state"] == "failed":
                with self.owned(c):
                    c.execute("update provider_calls set status='failed', updated_at=clock_timestamp() where request_id=%s", (rid,))
                    log(c, self.job, self.name, "provider_failed", name, request_id=rid, error=st["error"])
                raise Transient(f"provider failed {name}: {st['error']}")
            if time.monotonic() > deadline:
                with self.owned(c):
                    c.execute("update steps set status='waiting_provider',"
                              " next_attempt_at=clock_timestamp() + make_interval(secs => %s)"
                              " where job_id=%s and name=%s", (self.poll_s * 4, self.job, name))
                    log(c, self.job, self.name, "provider_slow", name, request_id=rid, waited_s=self.provider_wait_s,
                        note="not a failure: the same request is polled again, nothing is resubmitted")
                return None
            time.sleep(self.poll_s)

    def _fail_attempt(self, c, step, err):
        with self.owned(c):
            a = c.execute("select attempts from steps where job_id=%s and name=%s",
                          (self.job, step["name"])).fetchone()["attempts"]
            transient = isinstance(err, Transient)
            if not transient or a >= self.max_attempts:
                why = f"{type(err).__name__}: {err}" if not transient else str(err)
                msg = f"{step['name']} failed after {a} attempt{'s' if a != 1 else ''}: {why}"
                c.execute("update steps set status='failed', last_error=%s where job_id=%s and name=%s",
                          (msg, self.job, step["name"]))
                c.execute("update jobs set status='failed', error=%s, lease_owner=null, updated_at=clock_timestamp()"
                          " where id=%s", (msg, self.job))
                released = credits.release(c, self.job)
                log(c, self.job, self.name, "job_failed", step["name"], error=msg, reservation_released=released)
                return True
            delay = min(self.backoff_cap, self.backoff_base * 2 ** (a - 1))
            c.execute("update steps set next_attempt_at=clock_timestamp() + make_interval(secs => %s), last_error=%s"
                      " where job_id=%s and name=%s", (delay, str(err), self.job, step["name"]))
            log(c, self.job, self.name, "retry_scheduled", step["name"], attempt=a, next_in_s=delay,
                max_attempts=self.max_attempts, error=str(err))
            return False

    def _succeed(self, c):
        with self.owned(c):
            outputs = self._outputs(c)
            last = c.execute("select name from steps where job_id=%s order by seq desc limit 1", (self.job,)).fetchone()
            entry = credits.consume(c, self.job)
            c.execute("update jobs set status='succeeded', output=%s::jsonb, lease_owner=null, lease_expires_at=null,"
                      " updated_at=clock_timestamp() where id=%s", (json.dumps(outputs.get(last["name"])), self.job))
            log(c, self.job, self.name, "job_succeeded",
                charge=None if entry is None else {"id": entry["id"], "amount": entry["amount"],
                                                   "idempotency_key": entry["idempotency_key"]})
        return "succeeded"

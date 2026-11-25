"""Command line.

  python -m remake film            three openings, with the incidents of the recorded run
  python -m remake film --clean    same films, no injected incident
  python -m remake film --hooks A  one opening only
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from . import credits, db
from .pipeline import COST, STEP_LABELS, STEPS, FilmPipeline, ROOT
from .provider import SimProvider
from .worker import Worker, create_job, log

LEASE_S = 6.0
PROVIDER_DEADLINE_S = 300.0  # a clip with no answer this long after submission is cancelled and retried
JOB_DEADLINE_S = 3600.0  # a job still running this long after its first claim fails
ACCOUNT = "acct_demo_studio"
WEBHOOK_SECRET = "whsec_local_test_only"

# incident injected per opening in the recorded run
INCIDENTS = {
    "A": {"provider": {"clip:04": ["fail"]}, "story": "The clip provider fails the clip of beat 4; only that clip is redone."},
    "B": {"crash_at": "clip:02", "story": "The worker process is killed (SIGKILL) after the clip of beat 2; another worker takes over when the lease runs out."},
    "C": {"provider": {"clip:05": ["slow"]}, "story": "The clip provider answers the clip of beat 5 late; the same request is polled, nothing is resubmitted."},
}


def _event(eid, etype, obj):
    return {"id": eid, "type": etype, "data": {"object": obj}}


def buy_credits(dsn, amount=500):
    """A purchase as Stripe would report it: two event types for one payment, and
    the first one delivered twice at the same moment. Built and signed locally."""
    meta = {"account_id": ACCOUNT, "credits": str(amount)}
    evs = [_event("evt_local_checkout_1", "checkout.session.completed",
                  {"id": "cs_local_1", "payment_intent": "pi_local_1", "payment_status": "paid", "metadata": meta})] * 2
    evs.append(_event("evt_local_pi_1", "payment_intent.succeeded",
                      {"id": "pi_local_1", "status": "succeeded", "metadata": meta}))
    results = [None] * len(evs)

    def deliver(i, ev):
        payload = json.dumps(ev).encode()
        header = credits.sign(payload, WEBHOOK_SECRET)
        with db.connect(dsn) as c:
            results[i] = {"event_id": ev["id"], "type": ev["type"],
                          "effect": credits.handle_stripe_event(c, credits.verify(payload, header, WEBHOOK_SECRET))}

    threads = [threading.Thread(target=deliver, args=(i, ev)) for i, ev in enumerate(evs[:2])]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    deliver(2, evs[2])
    return results


def worker_for(dsn, name, pipe, faults=None, **kw):
    return Worker(dsn, name, pipe, SimProvider(dsn, pipe.render, faults or {}), lease_s=LEASE_S,
                  provider_deadline_s=PROVIDER_DEADLINE_S, status_timeout_s=60.0, job_deadline_s=JOB_DEADLINE_S, **kw)


def run_job(dsn, job_id, hook, pipe, clean):
    inc = {} if clean else INCIDENTS.get(hook, {})
    if inc.get("crash_at"):
        env = {**os.environ, "PYTHONPATH": str(ROOT)}
        r = subprocess.run([sys.executable, "-m", "remake", "work", "--dsn", dsn, "--job", str(job_id),
                            "--name", "worker-2", "--crash-at", inc["crash_at"]], env=env, cwd=ROOT)
        with db.connect(dsn) as c:
            log(c, job_id, "supervisor", "worker_process_exited", process="worker-2", returncode=r.returncode,
                note="killed with SIGKILL, its lease is left to expire")
        w = worker_for(dsn, "worker-3", pipe)
        t0 = time.monotonic()
        while True:
            got = w.run_once(only_job=job_id)
            if got:
                return got[1]
            if time.monotonic() - t0 > LEASE_S * 4:
                raise SystemExit(f"job {job_id} was never released")
            time.sleep(0.25)
    w = worker_for(dsn, "worker-1" if hook == "A" else "worker-4", pipe, inc.get("provider"))
    return w.run_once(only_job=job_id)[1]


def rel(v):
    """Paths inside the repository, written relative to it."""
    if isinstance(v, str) and v.startswith(str(ROOT) + os.sep):
        return str(Path(v).relative_to(ROOT))
    if isinstance(v, list):
        return [rel(x) for x in v]
    if isinstance(v, dict):
        return {k: rel(x) for k, x in v.items()}
    return v


def export(dsn, jobs, stripe, clean):
    with db.connect(dsn) as c:
        out = {"recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
               "postgres": c.execute("show server_version").fetchone()["server_version"],
               "lease_s": LEASE_S, "retry": {"base_s": 1.0, "cap_s": 8.0, "max_attempts": 4},
               "provider_deadline_s": PROVIDER_DEADLINE_S, "job_deadline_s": JOB_DEADLINE_S, "step_labels": STEP_LABELS,
               "credits_per_film": COST, "clean": clean, "stripe": stripe, "jobs": []}
        for hook, jid in jobs.items():
            j = c.execute("select * from jobs where id=%s", (jid,)).fetchone()
            t0 = j["created_at"]
            evs = c.execute("select * from events where job_id=%s order by id", (jid,)).fetchall()
            out["jobs"].append({
                "id": jid, "hook": hook, "status": j["status"], "fence": j["fence"], "error": j["error"],
                "incident": None if clean else INCIDENTS.get(hook, {}).get("story"),
                "output": rel(j["output"] or {}),
                "events": [{"t_ms": round((e["at"] - t0).total_seconds() * 1000), "worker": e["worker"],
                            "kind": e["kind"], "step": e["step"], "detail": rel(e["detail"])} for e in evs],
                "steps": [{"name": s["name"], "status": s["status"], "attempts": s["attempts"]}
                          for s in c.execute("select * from steps where job_id=%s order by seq", (jid,)).fetchall()],
                "provider_submissions": {r["step"]: r["n"] for r in c.execute(
                    "select step, count(*) as n from sim_provider.requests where client_key like %s group by step order by step",
                    (f"job{jid}:%",)).fetchall()},
                "hold": c.execute("select amount, status from credit_holds where job_id=%s", (jid,)).fetchone(),
            })
        out["ledger"] = [{"id": r["id"], "kind": r["kind"], "amount": r["amount"], "job_id": r["job_id"],
                          "idempotency_key": r["idempotency_key"]}
                         for r in c.execute("select * from ledger order by id").fetchall()]
        out["balance"] = credits.balance(c, ACCOUNT)
    return out


def cmd_film(args):
    hooks = [h.strip().upper() for h in args.hooks.split(",") if h.strip()]
    pipe = FilmPipeline(ROOT / "build")
    bad = [h for h in hooks if h not in pipe.script["hooks"]]
    if bad or not hooks:
        raise SystemExit(f"--hooks takes letters among {', '.join(pipe.script['hooks'])}; got {args.hooks!r}")
    with db.local_postgres() as dsn:
        db.reset(dsn)
        with db.connect(dsn) as c:
            c.execute("insert into accounts (id, name) values (%s, 'Demo studio')", (ACCOUNT,))
        stripe = buy_credits(dsn)
        for s in stripe:
            print(f"stripe {s['type']:<28} {s['event_id']:<22} {s['effect']}")
        jobs = {}
        with db.connect(dsn) as c:
            for h in hooks:
                jobs[h] = create_job(c, ACCOUNT, "film", {"hook": h, "out": f"out/remake_hook{h}.mp4"}, STEPS, COST)
        for h, jid in jobs.items():
            t = time.monotonic()
            status = run_job(dsn, jid, h, pipe, args.clean)
            print(f"job {jid} hook {h}: {status} in {time.monotonic() - t:.1f} s")
        rec = export(dsn, jobs, stripe, args.clean)
    stem = "clean_run" if args.clean else "recorded_run"
    if hooks != list(pipe.script["hooks"]):  # a partial run never replaces the full trace
        stem += "_" + "".join(hooks)
    path = ROOT / "runs" / f"{stem}.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(rec, indent=1, default=str))
    for j in rec["jobs"]:
        o = j["output"] or {}
        print(f"hook {j['hook']}: {j['status']}  {o.get('film', '-')}  {o.get('duration_s', '-')} s  {o.get('lufs', '-')} LUFS"
              f"  submissions {j['provider_submissions']}")
    print(f"balance {rec['balance']}  ->  {path.relative_to(ROOT)}")
    return 0 if all(j["status"] == "succeeded" for j in rec["jobs"]) else 1


def cmd_work(args):
    pipe = FilmPipeline(ROOT / "build")
    w = worker_for(args.dsn, args.name, pipe, crash_at=args.crash_at)
    got = w.run_once(only_job=args.job)
    print(f"{args.name}: {got}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m remake", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("film", help="render the remake through the job runner")
    f.add_argument("--hooks", default="A,B,C", help="openings to render, e.g. A or A,C (default A,B,C)")
    f.add_argument("--clean", action="store_true", help="no injected incident")
    f.set_defaults(fn=cmd_film)
    w = sub.add_parser("work", help="run one worker on one job (used by `film` for the killed-worker case)")
    w.add_argument("--dsn", required=True)
    w.add_argument("--job", type=int, required=True)
    w.add_argument("--name", default="worker")
    w.add_argument("--crash-at", default=None)
    w.set_defaults(fn=cmd_work)
    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

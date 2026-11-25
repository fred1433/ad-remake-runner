"""A fast stand-in for the film pipeline: same step names, no media work."""
import sys

from remake.provider import SimProvider
from remake.worker import Worker

STEPS = ["prep"] + [f"clip:{i:02d}" for i in range(1, 7)] + ["edit"]
COST = 60


class FakeFlow:
    def run(self, job, step, outputs):
        return {"step": step, "seen": sorted(outputs)}

    def clip_spec(self, job, step, outputs):
        return {"job": job["id"], "plan": int(step.split(":")[1])}

    def render(self, spec, request_id):
        return f"clip_{spec['plan']:02d}_{request_id}.mp4"


def make_worker(dsn, name, faults=None, lookup=True, **kw):
    flow = FakeFlow()
    kw.setdefault("lease_s", 1.0)
    kw.setdefault("backoff_base", 0.05)
    kw.setdefault("backoff_cap", 0.4)
    kw.setdefault("poll_s", 0.02)
    kw.setdefault("provider_wait_s", 0.05)
    return Worker(dsn, name, flow, SimProvider(dsn, flow.render, faults or {}, supports_lookup=lookup, slow_s=0.3), **kw)


if __name__ == "__main__":  # python -m tests.fakes DSN JOB CRASH_AT
    dsn, job, crash_at = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    make_worker(dsn, "worker-killed", crash_at=crash_at).run_once(only_job=job)

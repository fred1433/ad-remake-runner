"""python -m tests.kill_in_publish DSN JOB ROOT MODE

One worker on the fast flow, killed by SIGKILL the moment anything writes a file
under ROOT/out/: after a rename into out/ (MODE after_replace) or after a copy
into out/ (MODE after_stage). Kept from the review of commit cfa61ab, whose worker
renamed the film into out/ before its commit. Since then the worker writes nothing
there, so this process is not killed and runs the job to its end."""
import os
import shutil
import signal
import sys

from tests.fakes import FakeFlow, make_worker

dsn, job, root, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
out = os.path.join(os.path.abspath(root), "out") + os.sep
real_replace, real_copy = os.replace, shutil.copyfile


def _into_out(path):
    return os.path.abspath(os.fspath(path)).startswith(out)


def replace(a, b, *args, **kw):
    real_replace(a, b, *args, **kw)
    if mode == "after_replace" and _into_out(b):
        os.kill(os.getpid(), signal.SIGKILL)


def copyfile(a, b, *args, **kw):
    r = real_copy(a, b, *args, **kw)
    if mode == "after_stage" and _into_out(b):
        os.kill(os.getpid(), signal.SIGKILL)
    return r


os.replace, shutil.copyfile = replace, copyfile
make_worker(dsn, "killed", flow=FakeFlow(root)).run_once(only_job=job)

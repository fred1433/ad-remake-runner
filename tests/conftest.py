import json

import pytest

from remake import credits, db
from tests.fakes import COST, STEPS
from remake.worker import create_job

ACCOUNT = "acct_test"


@pytest.fixture(scope="session")
def pg():
    with db.local_postgres() as dsn:
        yield dsn


@pytest.fixture
def dsn(pg):
    db.reset(pg)
    with db.connect(pg) as c:
        c.execute("insert into accounts (id, name) values (%s, 'Test studio')", (ACCOUNT,))
        ev = {"id": "evt_seed", "type": "checkout.session.completed",
              "data": {"object": {"id": "cs_seed", "payment_intent": "pi_seed", "payment_status": "paid",
                                  "metadata": {"account_id": ACCOUNT, "credits": "500"}}}}
        credits.handle_stripe_event(c, ev)
    return pg


@pytest.fixture
def conn(dsn):
    with db.connect(dsn) as c:
        yield c


def new_job(conn, cost=COST):
    return create_job(conn, ACCOUNT, "film", {"hook": "A"}, STEPS, cost)


def rows(conn, sql, *args):
    return conn.execute(sql, args).fetchall()


def submissions(conn, job_id):
    return {r["step"]: r["n"] for r in conn.execute(
        "select step, count(*) as n from sim_provider.requests where client_key like %s group by step",
        (f"job{job_id}:%",)).fetchall()}


def charges(conn, job_id):
    return conn.execute("select * from ledger where job_id=%s and kind='charge'", (job_id,)).fetchall()


def events(conn, job_id, kind=None):
    q = "select * from events where job_id=%s" + (" and kind=%s" if kind else "") + " order by id"
    return conn.execute(q, (job_id, kind) if kind else (job_id,)).fetchall()

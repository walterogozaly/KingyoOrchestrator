"""CTE / subquery flattening: strategies are right, and incremental == full rebuild on those shapes.
Run: python -m pytest -q tests/test_flatten.py"""

from pathlib import Path

import duckdb
import pytest

import kingyo_orchestrator.incremental.analysis as A
from kingyo_orchestrator.incremental import flatten
from kingyo_orchestrator.incremental.demo import seed, snapshot
from kingyo_orchestrator.incremental.executor import Orchestrator
from kingyo_orchestrator.incremental.graph import Dag
from kingyo_orchestrator.incremental.sqlx import Model, load_repo

flatten.install()
REPO = str(Path(__file__).parent / "flatten_repo")
P = Model("p", "table", "", partition_expr="DATE(created_at)")


def strat(sql, part="DATE(created_at)", parent=P):
    return A.analyze_edge(Model("c", "table", sql, partition_expr=part), parent)


@pytest.mark.parametrize(
    "sql,kind",
    [
        (
            "with b as (select * from p), a as (select id, created_at from b where x > 1) select a.id, a.created_at from a",
            "aligned",
        ),
        (
            "select s.id, s.created_at from (select id, created_at from p where x > 1) s join d on d.id = s.id",
            "aligned",
        ),
        ("select * from (select * from (select id, created_at from p) q) r", "aligned"),
        (
            "with a as (select *, row_number() over (partition by date(created_at) order by id) rn from p) select id, created_at from a qualify rn = 1",
            "aligned",
        ),
        (
            "with a as (select id, date(created_at) as d, created_at from p) select id, created_at from a",
            "aligned",
        ),
        (
            "with a as (select id, created_at, extract(hour from created_at) h from p) select distinct id, created_at, h from a",
            "aligned",
        ),
        # not provable -> must stay full
        (
            "with a as (select * from p) select a.id, b.created_at from a join a as b using (id)",
            "full",
        ),  # parent read twice
        (
            "select id, created_at from p where id in (select id from p where x > 1)",
            "full",
        ),  # parent in subquery expr
        ("with a as (select id, created_at from p order by id limit 3) select * from a", "full"),
        (
            "with a as (select id, created_at, row_number() over (partition by id order by created_at) rn from p) select id, created_at from a where rn = 1",
            "full",
        ),
        (
            "with a as (select count(*) n, max(created_at) created_at from p) select * from a",
            "full",
        ),
        (
            "select a.id, a.created_at from d as a left join (select id from p) b on a.id = b.id",
            "full",
        ),  # parent null-extended
        (
            "with a as (select id, created_at from p union all select id, created_at from p) select * from a",
            "full",
        ),
        (
            "with a as (select distinct id from p) select a.id, current_date() created_at from a",
            "full",
        ),
    ],
)
def test_strategy(sql, kind):
    assert strat(sql).kind == kind


def test_partition_provenance_is_traced():
    s = strat(
        "with a as (select id, created_at as ts from p) select id, ts from a", part="DATE(ts)"
    )
    assert s.kind == "aligned" and A._norm(s.parent_expr) == "date(created_at)"
    s = strat(
        "with a as (select id, created_at as ts from p) select id, ts from a", part="id"
    )  # unrelated partition col
    assert s.kind == "data_dependent"


def test_strategies_on_flatten_repo():
    models = load_repo(REPO)
    kinds = {
        n: A.analyze_edge(models[n], models["orders"]).kind
        for n in models
        if "orders" in models[n].deps
    }
    assert kinds == {
        "cte_chain": "aligned",
        "sub_join": "aligned",
        "cte_window": "aligned",
        "cte_by_order_date": "data_dependent",
        "cte_group_by_id": "full",
        "cte_limit": "full",
        "in_subquery": "full",
    }
    assert (
        A.analyze_edge(models["sub_join"], models["customers"]).kind == "full"
    )  # dimension on nullable side


def _setup():
    dag, con = Dag(load_repo(REPO)), duckdb.connect()
    seed(con)
    o = Orchestrator(dag, con)
    o.build_all()
    return dag, con, o


def _check(dag, con, o):
    got = snapshot(con, dag)
    o.build_all()
    assert got == snapshot(con, dag)


def test_incremental_equals_full_rebuild_through_ctes():
    dag, con, o = _setup()
    con.execute("DELETE FROM orders WHERE order_id = 1")
    con.execute("""INSERT INTO orders VALUES
        (1,1,15.0,'ok',DATE '2026-10-01',TIMESTAMP '2026-10-03 08:00:00'),
        (5,2,50.0,'ok',DATE '2026-10-03',TIMESTAMP '2026-10-03 09:00:00'),
        (6,2,3.0,'ok',DATE '2026-10-03',TIMESTAMP '2026-10-03 10:00:00')""")
    rep = o.on_change("orders", ["2026-10-03"])
    # partition-local through CTE / subquery: only the signalled partition and the superseded row's old one
    for n in ("cte_chain", "sub_join", "cte_window"):
        assert {"2026-10-03"} <= rep.propagated[n] <= {"2026-10-01", "2026-10-03"}, n
    assert not any(
        "ALL dirty" in s for s in rep.steps if "orders->cte_chain" in s or "orders->sub_join" in s
    )
    _check(dag, con, o)


def test_random_upserts_match_full_rebuild():
    import random
    from datetime import datetime, timedelta

    from kingyo_orchestrator.incremental.executor import Policy

    T = datetime(2026, 10, 4, 21, 10)
    for seed_ in range(8):
        rnd = random.Random(seed_)
        dag, con, o = _setup()
        nxt = 100
        for _ in range(6):
            day = 1 + rnd.randrange(5)
            touched = set()
            for _ in range(rnd.randrange(1, 4)):
                ts = f"2026-10-{day:02d} {rnd.randrange(24):02d}:00:00"
                if rnd.random() < 0.5:
                    oid, cust, nxt = nxt, rnd.randrange(1, 5), nxt + 1
                else:
                    oid = rnd.choice(
                        [r[0] for r in con.execute("SELECT order_id FROM orders").fetchall()]
                    )
                    cust = con.execute(
                        "SELECT customer_id FROM orders WHERE order_id=?", [oid]
                    ).fetchone()[0]
                    if (
                        str(
                            con.execute(
                                "SELECT last_upd_ts FROM orders WHERE order_id=?", [oid]
                            ).fetchone()[0]
                        )
                        >= ts
                    ):
                        continue
                    con.execute("DELETE FROM orders WHERE order_id=?", [oid])
                con.execute(
                    "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                    [
                        oid,
                        cust,
                        float(rnd.randrange(1, 90)),
                        rnd.choice(["ok", "ok", "test"]),
                        f"2026-10-{rnd.randrange(1, 6):02d}",
                        ts,
                    ],
                )
                touched.add(f"2026-10-{day:02d}")
            if touched:
                o.signal("orders", sorted(touched), T)
                o.run_once(T + timedelta(hours=1), Policy(settle=timedelta(0)))
                assert o.status() == []
                _check(dag, con, o)

"""Column-aware change relevance: a change only propagates to children that read the changed columns."""

import random
from datetime import datetime

import duckdb
import pytest

from kingyo_orchestrator.incremental import columns as C
from kingyo_orchestrator.incremental.demo import SAMPLE_REPO, seed, snapshot
from kingyo_orchestrator.incremental.executor import ALL, Orchestrator
from kingyo_orchestrator.incremental.graph import Dag
from kingyo_orchestrator.incremental.sqlx import Model, load_repo

NOW = datetime(2026, 10, 3, 21, 30)
SEEN = datetime(2026, 10, 3, 21, 0)


def _setup():
    dag, con = Dag(load_repo(SAMPLE_REPO)), duckdb.connect()
    seed(con)
    con.execute("ALTER TABLE orders ADD COLUMN note VARCHAR DEFAULT 'n'")  # a column no model reads
    o = Orchestrator(dag, con)
    o.build_all()
    return dag, con, o


def _check(dag, con, o):
    got = snapshot(con, dag)
    o.build_all()
    assert got == snapshot(con, dag)


def test_unread_column_change_is_a_no_op():
    dag, con, o = _setup()
    con.execute("UPDATE orders SET note = 'edited' WHERE order_id = 2")
    o.signal("orders", ["2026-10-01"], SEEN, columns=["note"])
    rep = o.run_once(NOW)
    assert rep.propagated == {} and o.status() == []
    assert any("do not reach stg_orders" in s for s in rep.steps)
    _check(dag, con, o)


def test_value_column_only_reaches_models_that_read_it():
    dag, con, o = _setup()
    con.execute("UPDATE customers SET segment = 'mid' WHERE customer_id = 2")
    o.signal("customers", ALL, SEEN, columns=["segment"])
    rep = o.run_once(NOW)
    # orders_enriched only selects segment -> its marks carry {segment}; segment_revenue groups by it -> all
    assert "orders_enriched" in rep.propagated
    _check(dag, con, o)


def test_structural_column_propagates_everything():
    dag, con, o = _setup()
    con.execute(
        "UPDATE orders SET status = 'test' WHERE order_id = 1"
    )  # filtered by stg_orders' WHERE
    o.signal("orders", ["2026-10-01"], SEEN, columns=["status"])
    rep = o.run_once(NOW)
    assert "stg_orders" in rep.propagated
    _check(dag, con, o)


def test_unknown_columns_merge_to_all():
    dag, con, o = _setup()
    o.signal("orders", ["2026-10-01"], SEEN, columns=["note"])
    o.signal("orders", ["2026-10-01"], SEEN)  # a second signal that doesn't know the columns wins
    assert o.dirty_cols("orders") is None
    o2 = Orchestrator(dag, con)
    o2.signal("orders", ["2026-10-02"], SEEN, columns=["note"])
    o2.signal("orders", ["2026-10-02"], SEEN, columns=["amount"])
    assert o2.dirty_cols("orders", ["2026-10-02"]) == {"amount", "note"}


@pytest.mark.parametrize("seed_", range(10))
def test_random_column_updates_match_full_rebuild(seed_):
    rnd = random.Random(seed_)
    dag, con, o = _setup()
    for _ in range(3):
        col = rnd.choice(["amount", "status", "note", "customer_id"])
        oid = rnd.randint(1, 4)
        val = {
            "amount": rnd.choice([1.0, 99.0, 30.0]),
            "status": rnd.choice(["'ok'", "'test'"]),
            "note": "'x'",
            "customer_id": rnd.randint(1, 3),
        }[col]
        con.execute(f"UPDATE orders SET {col} = {val} WHERE order_id = {oid}")
        day = con.execute(
            f"SELECT CAST(DATE(last_upd_ts) AS VARCHAR) FROM orders WHERE order_id={oid}"
        ).fetchone()[0]
        o.signal("orders", [day], SEEN, columns=[col])
        o.run_once(NOW)
        _check(dag, con, o)


def _m(sql):
    return Model("x", "table", sql, ["orders"], None, None)


ORD = Model("orders", "declaration", "", [], "DATE(last_upd_ts)", "order_id")
COLS = {"orders": ["order_id", "customer_id", "amount", "status", "note", "last_upd_ts"]}


@pytest.mark.parametrize(
    "sql,changed,want",
    [
        ("select order_id, amount*2 a2 from orders where status<>'x'", {"amount"}, {"a2"}),
        ("select order_id, amount*2 a2 from orders where status<>'x'", {"note"}, set()),
        ("select order_id, amount*2 a2 from orders where status<>'x'", {"status"}, None),
        (
            "with c as (select customer_id, sum(amount) amt from orders group by customer_id) select * from c",
            {"amount"},
            {"amt"},
        ),
        (
            "with c as (select customer_id, sum(amount) amt from orders group by customer_id) select * from c",
            {"customer_id"},
            None,
        ),
        (
            "with c as (select order_id, amount from orders) select order_id from c where amount>5",
            {"amount"},
            None,
        ),
        (
            "select order_id from orders where customer_id in (select customer_id from orders where amount>1)",
            {"amount"},
            None,
        ),
        (
            "select order_id from (select order_id, row_number() over (partition by customer_id order by amount) rn "
            "from orders) where rn=1",
            {"amount"},
            None,
        ),
        (
            "select order_id, amount from orders union all select order_id, 0 from orders",
            {"amount"},
            None,
        ),
        ("select * from orders", {"note"}, {"note"}),
        ("select count(*) n from orders", {"amount"}, set()),
    ],
)
def test_changed_outputs(sql, changed, want):
    assert C.changed_outputs(_m(sql), ORD, changed, COLS) == want

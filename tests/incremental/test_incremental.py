"""Core invariant: incremental result == full rebuild. Run: python -m pytest -q"""

from datetime import datetime, timedelta

import duckdb
import pytest

from kingyo_orchestrator.incremental.demo import SAMPLE_REPO, seed, snapshot
from kingyo_orchestrator.incremental.executor import ALL, Orchestrator, Policy
from kingyo_orchestrator.incremental.graph import Dag
from kingyo_orchestrator.incremental.sqlx import load_repo


def _setup(append_only=False):
    models = load_repo(SAMPLE_REPO)
    if append_only:
        models["orders"].unique_key = None
    dag, con = Dag(models), duckdb.connect()
    seed(con)
    o = Orchestrator(dag, con)
    o.build_all()
    return dag, con, o


def _check(dag, con, o):
    got = snapshot(con, dag)
    o.build_all()
    assert got == snapshot(con, dag)


def test_upsert_with_late_update():
    dag, con, o = _setup()
    con.execute("DELETE FROM orders WHERE order_id = 1")
    con.execute("""INSERT INTO orders VALUES
        (1,1,15.0,'ok',DATE '2026-10-01',TIMESTAMP '2026-10-03 08:00:00'),
        (5,2,50.0,'ok',DATE '2026-10-03',TIMESTAMP '2026-10-03 09:00:00')""")
    o.on_change("orders", ["2026-10-03"])
    _check(dag, con, o)


def test_append_only_new_day():
    dag, con, o = _setup(append_only=True)
    con.execute(
        "INSERT INTO orders VALUES (9,3,7.0,'ok',DATE '2026-10-03',TIMESTAMP '2026-10-03 09:00:00')"
    )
    rep = o.on_change("orders", ["2026-10-03"])
    assert rep.propagated["stg_orders"] == {"2026-10-03"}  # only one partition touched
    _check(dag, con, o)


def test_early_cutoff_when_nothing_changes():
    dag, con, o = _setup(append_only=True)
    con.execute(
        "INSERT INTO orders VALUES (9,3,7.0,'test',DATE '2026-10-03',TIMESTAMP '2026-10-03 09:00:00')"
    )
    rep = o.on_change("orders", ["2026-10-03"])  # filtered out by status != 'test'
    assert rep.propagated == {}
    _check(dag, con, o)


def test_dimension_change_full_refresh():
    dag, con, o = _setup()
    con.execute("UPDATE customers SET segment='ent' WHERE customer_id=1")
    o.on_change("customers", ALL)
    _check(dag, con, o)


# ---- wake-up / persistence behaviour ----------------------------------------------

T = datetime(2026, 10, 4, 21, 10)


def _upsert(con):
    con.execute("DELETE FROM orders WHERE order_id = 1")
    con.execute("""INSERT INTO orders VALUES
        (1,1,15.0,'ok',DATE '2026-10-01',TIMESTAMP '2026-10-03 08:00:00'),
        (5,2,50.0,'ok',DATE '2026-10-03',TIMESTAMP '2026-10-03 09:00:00')""")


def test_fresh_signal_waits_then_acts_on_later_wake(tmp_path):
    db = str(tmp_path / "wh.duckdb")
    dag, con, o = _setup()  # in-memory for seeding; copy approach not needed: re-seed file db
    con2 = duckdb.connect(db)
    seed(con2)
    Orchestrator(dag, con2).build_all()
    _upsert(con2)
    Orchestrator(dag, con2).signal("orders", ["2026-10-03"], T)
    con2.close()
    # wake 1: signal only 5 min old -> nothing happens, state kept
    con2 = duckdb.connect(db)
    rep = Orchestrator(dag, con2).run_once(T + timedelta(minutes=5), Policy())
    assert "orders" in rep.deferred and rep.propagated == {}
    con2.close()
    # wake 2, brand new process/connection: settled -> done, dirty set empty
    con2 = duckdb.connect(db)
    o2 = Orchestrator(dag, con2)
    o2.run_once(T + timedelta(minutes=20), Policy())
    assert o2.status() == []
    _check(dag, con2, o2)
    assert o2.run_once(T + timedelta(minutes=40)).steps == ["nothing dirty: no-op"]


def test_crash_mid_run_is_resumable():
    dag, con, o = _setup()
    _upsert(con)
    o.signal("orders", ["2026-10-03"], T)
    real = o._refresh

    def boom(name, *a, **k):
        if name == "orders_by_order_date":
            raise RuntimeError("killed")
        return real(name, *a, **k)

    o._refresh = boom
    with pytest.raises(RuntimeError):
        o.run_once(T + timedelta(hours=1), Policy())
    assert any(m == "orders_by_order_date" for m, _, _ in o.status())  # debt survived
    o._refresh = real
    o.run_once(T + timedelta(hours=2), Policy())
    assert o.status() == []
    _check(dag, con, o)


def test_budget_defers_remaining_work():
    dag, con, o = _setup()
    _upsert(con)
    o.signal("orders", ["2026-10-03"], T)
    rep = o.run_once(T + timedelta(hours=1), Policy(budget_seconds=-1))
    assert rep.deferred and o.status()
    o.run_once(T + timedelta(hours=2), Policy())
    assert o.status() == []
    _check(dag, con, o)


def test_many_dirty_partitions_trigger_full_refresh():
    dag, con, o = _setup(append_only=True)
    o.signal("orders", ["2026-10-01", "2026-10-02"], T)
    rep = o.run_once(T + timedelta(hours=1), Policy(min_rows_for_cost_rules=0))
    assert any("FULL refresh" in s for s in rep.steps)
    _check(dag, con, o)


def test_key_capture_skipped_when_child_rebuilds_anyway():
    # append-only source: no row-level delta, so keyed children would get whole-partition keys
    dag, con, o = _setup(append_only=True)
    con.execute("""INSERT INTO orders SELECT 100 + i, 1 + i % 3, i, 'ok', DATE '2026-09-20' + CAST(i AS INTEGER),
        TIMESTAMP '2026-09-20 08:00:00' + INTERVAL (i) DAY FROM range(10) t(i)""")
    o.build_all()
    con.execute("""INSERT INTO orders VALUES (201, 1, 5.0, 'ok', DATE '2026-10-02', TIMESTAMP '2026-10-02 19:00:00'),
        (202, 2, 6.0, 'ok', DATE '2026-10-02', TIMESTAMP '2026-10-02 20:00:00')""")
    o.signal("orders", ["2026-10-02"], T)
    # the parent stays partial (1 of 13 partitions); the keyed children's changed keys exceed 25% of rows.
    # min_rows_for_cost_rules=0 makes the tiny tables count as big, which also turns content cutoff off
    rep = o.run_once(T + timedelta(hours=1), Policy(min_rows_for_cost_rules=0))
    assert any("stg_orders: partitions" in s for s in rep.steps)  # the parent itself stays partial
    assert any("skipping key capture" in s for s in rep.steps)
    assert any("too many changed keys -> latest_status ALL dirty" in s for s in rep.steps)
    _check(dag, con, o)


def test_key_count_rule_waits_for_content_cutoff():
    """With content cutoff on, rewritten partitions that did not change are not propagated, so the
    up-front key count (which would cover all of them) must not force a full rebuild."""
    dag, con, o = _setup()
    con.execute("""INSERT INTO orders SELECT 100 + i, 1 + i % 3, i, 'ok', DATE '2026-09-20' + CAST(i AS INTEGER),
        TIMESTAMP '2026-09-20 08:00:00' + INTERVAL (i) DAY FROM range(10) t(i)""")
    o.build_all()
    con.execute("DELETE FROM orders WHERE order_id = 101")
    con.execute(
        "INSERT INTO orders VALUES (101, 2, 99.0, 'ok', DATE '2026-09-21', TIMESTAMP '2026-10-03 09:00:00')"
    )
    o.signal("orders", ["2026-10-03"], T)
    rep = o.run_once(
        T + timedelta(hours=1), Policy(min_rows_for_cost_rules=0, cutoff_on_partial=True)
    )
    assert not any("skipping key capture" in s for s in rep.steps)
    _check(dag, con, o)


def test_random_upserts_match_full_rebuild():
    """Property test: random inserts / forward-moving updates / filtered rows, signalled by partition."""
    import random

    for seed_ in range(10):
        rnd = random.Random(seed_)
        dag, con, o = _setup()
        nxt = 100
        for _step in range(6):
            day = 1 + rnd.randrange(5)
            touched = set()
            for _ in range(rnd.randrange(1, 4)):
                ts = f"2026-10-{day:02d} {rnd.randrange(24):02d}:00:00"
                if rnd.random() < 0.5:
                    oid, cust = nxt, rnd.randrange(1, 5)
                    nxt += 1
                else:
                    oid = rnd.choice(
                        [r[0] for r in con.execute("SELECT order_id FROM orders").fetchall()]
                    )
                    cust = con.execute(
                        "SELECT customer_id FROM orders WHERE order_id=?", [oid]
                    ).fetchone()[0]
                    old = con.execute(
                        "SELECT last_upd_ts FROM orders WHERE order_id=?", [oid]
                    ).fetchone()[0]
                    if str(old) >= ts:
                        continue
                    con.execute("DELETE FROM orders WHERE order_id=?", [oid])
                status = rnd.choice(["ok", "ok", "test"])
                con.execute(
                    "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                    [
                        oid,
                        cust,
                        float(rnd.randrange(1, 90)),
                        status,
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


def test_check_reports_patterns_and_fixes():
    from kingyo_orchestrator.incremental.cli import check

    dag, con, o = _setup()
    out = check(o)
    assert "latest_orders" in out and "keyed" in out and "outer_join" in out
    assert "fix: add bigquery.partitionBy" in out and "customers is unpartitioned" in out


def test_row_deltas_with_key_changes_match_full_rebuild():
    """Updates that move an order to another customer: keyed children must recompute both the old and
    the new customer (pre- and post-image of the row-level delta)."""
    import random

    used = 0
    for seed_ in range(20):
        rnd = random.Random(seed_)
        dag, con, o = _setup()
        for step in range(5):
            ts = f"2026-10-{3 + step:02d} {rnd.randrange(24):02d}:00:00"
            day = ts[:10]
            for _ in range(rnd.randrange(1, 4)):
                oid = rnd.choice(
                    [r[0] for r in con.execute("SELECT order_id FROM orders").fetchall()]
                )
                con.execute("DELETE FROM orders WHERE order_id=?", [oid])
                con.execute(
                    "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                    [
                        oid,
                        rnd.randrange(1, 5),
                        float(rnd.randrange(1, 90)),
                        rnd.choice(["ok", "ok", "test"]),
                        day,
                        ts,
                    ],
                )
            o.signal("orders", [day], T)
            rep = o.run_once(datetime(2026, 10, 9))
            used += any("keys of changed rows" in s for s in rep.steps)
            _check(dag, con, o)
    assert used

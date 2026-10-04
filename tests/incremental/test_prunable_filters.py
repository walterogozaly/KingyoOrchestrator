"""Every partition filter Kingyo emits must be statically prunable: literal values on the partition column,
never `CAST(... AS VARCHAR) IN (...)` or `IN (SELECT ...)` on a partition expression."""

import re
from datetime import datetime, timedelta

import duckdb

from kingyo_orchestrator.incremental.demo import SAMPLE_REPO, seed
from kingyo_orchestrator.incremental.executor import Orchestrator, Policy
from kingyo_orchestrator.incremental.graph import Dag
from kingyo_orchestrator.incremental.predicates import part_pred
from kingyo_orchestrator.incremental.sqlx import load_repo


class Recorder:
    def __init__(self, con):
        self._c, self.sql = con, []

    def execute(self, q, *a):
        self.sql.append(" ".join(str(q).split()))
        return self._c.execute(q, *a)

    def __getattr__(self, k):
        return getattr(self._c, k)


def test_part_pred_shapes():
    assert part_pred("DATE(created_at)", ["2026-10-01", "2026-10-02", "2026-10-05"]) == (
        "((created_at >= TIMESTAMP '2026-10-01 00:00:00' AND created_at < TIMESTAMP '2026-10-03 00:00:00') OR "
        "(created_at >= TIMESTAMP '2026-10-05 00:00:00' AND created_at < TIMESTAMP '2026-10-06 00:00:00'))"
    )
    assert (
        part_pred("order_date", ["2026-10-01", "2026-10-02"])
        == "(order_date IN (DATE '2026-10-01', DATE '2026-10-02'))"
    )


def test_no_unprunable_partition_filters_in_a_run():
    dag = Dag(load_repo(SAMPLE_REPO))
    con = duckdb.connect()
    seed(con)
    o = Orchestrator(dag, con)
    o.build_all()
    con.execute("DELETE FROM orders WHERE order_id = 1")
    con.execute("""INSERT INTO orders VALUES (1,1,15.0,'ok',DATE '2026-10-01',TIMESTAMP '2026-10-03 08:00:00'),
                                             (5,2,50.0,'ok',DATE '2026-10-03',TIMESTAMP '2026-10-03 09:00:00')""")
    t = datetime(2026, 10, 4)
    o.signal("orders", ["2026-10-03"], t)
    o.con = rec = Recorder(con)
    o.run_once(t + timedelta(hours=1), Policy(settle=timedelta(0)))
    exprs = {m.partition_expr for m in dag.models.values() if m.partition_expr}
    bad = []
    for q in rec.sql:
        where = q.split(" WHERE ", 1)[1] if " WHERE " in q else ""
        for e in exprs:
            if re.search(re.escape(f"CAST({e} AS VARCHAR) IN"), where) or re.search(
                re.escape(e) + r"\) IN \(SELECT", where
            ):
                bad.append(q)
    assert not bad, "\n".join(bad)

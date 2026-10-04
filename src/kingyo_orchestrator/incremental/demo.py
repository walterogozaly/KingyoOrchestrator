"""python demo.py : build the sample repo, land new data, signal the change, verify vs full rebuild."""

from pathlib import Path

import duckdb

from kingyo_orchestrator.incremental.executor import Orchestrator, explain
from kingyo_orchestrator.incremental.graph import Dag
from kingyo_orchestrator.incremental.sqlx import load_repo

SAMPLE_REPO = str(Path(__file__).parent / "sample_repo")


def seed(con):
    con.execute(
        "CREATE TABLE customers AS SELECT * FROM (VALUES (1,'smb'),(2,'ent'),(3,'smb')) t(customer_id, segment)"
    )
    con.execute("""CREATE TABLE orders AS SELECT * FROM (VALUES
        (1, 1, 10.0, 'ok',   DATE '2026-10-01', TIMESTAMP '2026-10-01 09:00:00'),
        (2, 2, 20.0, 'ok',   DATE '2026-10-01', TIMESTAMP '2026-10-01 10:00:00'),
        (3, 3, 30.0, 'ok',   DATE '2026-10-02', TIMESTAMP '2026-10-02 11:00:00'),
        (4, 1, 40.0, 'test', DATE '2026-10-02', TIMESTAMP '2026-10-02 12:00:00')
    ) t(order_id, customer_id, amount, status, order_date, last_upd_ts)""")


def snapshot(con, dag):
    return {
        n: sorted(map(str, con.execute(f"SELECT * FROM {n}").fetchall()))
        for n in dag.order
        if dag.models[n].type != "declaration"
    }


def main():
    dag = Dag(load_repo(SAMPLE_REPO))
    con = duckdb.connect()
    seed(con)
    orch = Orchestrator(dag, con)
    orch.build_all()
    print("== static edge analysis ==\n" + explain(dag, orch.columns()) + "\n")

    # new data lands: a new order on 10-03, plus a late update of order 1 (order_date 10-01, last_upd_ts 10-03)
    con.execute("DELETE FROM orders WHERE order_id = 1")
    con.execute("""INSERT INTO orders VALUES
        (1, 1, 15.0, 'ok', DATE '2026-10-01', TIMESTAMP '2026-10-03 08:00:00'),
        (5, 2, 50.0, 'ok', DATE '2026-10-03', TIMESTAMP '2026-10-03 09:00:00')""")
    rep = orch.on_change("orders", ["2026-10-03"])
    print("== incremental run ==\n" + "\n".join(rep.steps) + "\n")

    got = snapshot(con, dag)
    orch.build_all()
    want = snapshot(con, dag)
    bad = [n for n in want if got[n] != want[n]]
    print("MATCHES FULL REBUILD" if not bad else f"MISMATCH in {bad}")
    for n in bad:
        print(n, "\n incremental:", got[n], "\n full:", want[n])


if __name__ == "__main__":
    main()

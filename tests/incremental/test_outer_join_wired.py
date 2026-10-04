"""Outer-join strategy wired into the orchestrator: incremental == full rebuild when a partitioned
dimension on the nullable side of a LEFT JOIN changes (rows updated in place, moving partitions)."""

import random
from datetime import datetime, timedelta

import duckdb

from kingyo_orchestrator.incremental.executor import Orchestrator, Policy
from kingyo_orchestrator.incremental.graph import Dag
from kingyo_orchestrator.incremental.sqlx import load_repo

T = datetime(2026, 10, 4, 21, 0)


def _repo(tmp_path):
    d = tmp_path / "definitions"
    d.mkdir()
    (d / "facts.sqlx").write_text(
        'config { type: "declaration", name: "facts", bigquery: { partitionBy: "DATE(ts)" } }'
    )
    (d / "dims.sqlx").write_text(
        'config { type: "declaration", name: "dims", uniqueKey: "id", bigquery: { partitionBy: "DATE(upd)" } }'
    )
    (d / "enriched.sqlx").write_text(
        'config { type: "table", bigquery: { partitionBy: "DATE(ts)" } }\n'
        'select f.fid, f.ts, f.dim_id, d.label from ${ref("facts")} f left join ${ref("dims")} d on f.dim_id = d.id'
    )
    return tmp_path


def _snap(con):
    return sorted(map(str, con.execute("SELECT * FROM enriched").fetchall()))


def test_dimension_update_on_nullable_side(tmp_path):
    dag = Dag(load_repo(_repo(tmp_path)))
    assert dag.models["enriched"].deps == ["facts", "dims"]
    for seed in range(15):
        rnd = random.Random(seed)
        con = duckdb.connect()
        con.execute("CREATE TABLE facts(fid INT, ts TIMESTAMP, dim_id INT)")
        con.execute("CREATE TABLE dims(id INT, label VARCHAR, upd TIMESTAMP)")
        for i in range(30):
            con.execute(
                "INSERT INTO facts VALUES (?, ?, ?)",
                [i, f"2026-10-0{1 + i % 5} 01:00:00", rnd.randrange(8)],
            )
        for i in range(5):
            con.execute("INSERT INTO dims VALUES (?, ?, '2026-10-01 00:00:00')", [i, f"l{i}"])
        o = Orchestrator(dag, con)
        o.build_all()
        from kingyo_orchestrator.incremental.analysis import analyze_edge

        assert (
            analyze_edge(dag.models["enriched"], dag.models["dims"], o.columns()).kind
            == "outer_join"
        )
        for step in range(5):
            day = f"2026-10-0{2 + step}"
            for _ in range(rnd.randrange(1, 3)):
                i = rnd.randrange(8)  # ids 5..7 are new dimension rows (NULL -> value)
                con.execute("DELETE FROM dims WHERE id=?", [i])
                con.execute(
                    "INSERT INTO dims VALUES (?, ?, ?)", [i, f"l{i}_{step}", f"{day} 03:00:00"]
                )
            o.signal("dims", [day], T)
            o.run_once(T + timedelta(hours=1), Policy(settle=timedelta(0), full_refresh_ratio=2))
            assert o.status() == []
            got = _snap(con)
            con.execute(f"CREATE OR REPLACE TABLE enriched AS {dag.models['enriched'].sql}")
            assert got == _snap(con), (seed, step)

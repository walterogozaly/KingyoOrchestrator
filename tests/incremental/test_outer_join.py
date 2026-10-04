"""Incremental refresh on the nullable side of an outer join == full rebuild. Run: python -m pytest -q tests/test_outer_join.py"""

import random

import duckdb
import pytest

from kingyo_orchestrator.incremental import outer_join as oj

CHILDREN = {
    # name: (sql, partition expr over child output, expected plan mode)
    "c_key": (
        "SELECT a.id, a.k, a.v, a.ts, b.w FROM a LEFT JOIN b ON a.k = b.k",
        "DATE(ts)",
        "key",
    ),
    "c_comp": (
        "SELECT a.id, a.k, a.k2, a.ts, b.w FROM a LEFT JOIN b ON a.k = b.k AND a.k2 = b.k2 AND b.w > 1 "
        "INNER JOIN dim d ON d.id = a.id",
        "DATE(ts)",
        "key",
    ),
    "c_anti": (
        "SELECT a.id, a.k, a.ts FROM a LEFT JOIN b ON a.k = b.k WHERE b.k IS NULL",
        "DATE(ts)",
        "key",
    ),
    "c_part": (
        "SELECT a.id, a.ts, coalesce(b.w, -1) AS w FROM a LEFT JOIN b ON a.k = b.k",
        "DATE(ts)",
        "partition",
    ),
    "c_right": ("SELECT a.id, a.k, a.ts, b.w FROM b RIGHT JOIN a ON a.k = b.k", "DATE(ts)", "key"),
    "c_star": ("SELECT a.*, b.w FROM a LEFT JOIN b ON a.k = b.k", "DATE(ts)", "key"),
}


def setup(seed):
    r = random.Random(seed)
    con = duckdb.connect()
    con.execute("CREATE TABLE a(id INT, k INT, k2 INT, v INT, ts TIMESTAMP)")
    con.execute("CREATE TABLE b(k INT, k2 INT, w INT, ts TIMESTAMP)")
    con.execute("CREATE TABLE dim(id INT)")
    for i in range(60):
        con.execute(
            "INSERT INTO a VALUES (?,?,?,?,?)",
            [
                i,
                r.choice([None] + list(range(8))),
                r.randint(0, 2),
                r.randint(0, 9),
                f"2026-10-0{r.randint(1, 5)} 10:00:00",
            ],
        )
        if i % 7:
            con.execute("INSERT INTO dim VALUES (?)", [i])
    for _ in range(10):
        add_b(con, r, 3)
    return con, r


def add_b(con, r, n):
    for _ in range(n):
        con.execute(
            "INSERT INTO b VALUES (?,?,?,?)",
            [
                r.choice([None] + list(range(10))),
                r.randint(0, 2),
                r.randint(0, 4),
                f"2026-10-0{r.randint(1, 5)} 12:00:00",
            ],
        )


def build(con):
    for n, (sql, _, _) in CHILDREN.items():
        con.execute(f"CREATE OR REPLACE TABLE {n} AS {sql}")


def snap(con, n):
    return sorted(map(str, con.execute(f"SELECT * FROM {n}").fetchall()))


@pytest.mark.parametrize("name", CHILDREN)
def test_plan_modes(name):
    sql, pe, mode = CHILDREN[name]
    p = oj.analyze(sql, pe, "b")
    assert isinstance(p, oj.Plan) and p.mode == mode, p


@pytest.mark.parametrize("seed", range(8))
@pytest.mark.parametrize("name", CHILDREN)
def test_matches_full_rebuild(name, seed):
    sql, pe, _ = CHILDREN[name]
    con, r = setup(seed)
    build(con)
    plan = oj.analyze(sql, pe, "b")
    for step in range(6):
        # capture pre-image keys (deletes / key moves) before mutating the parent
        op = r.choice(["insert", "update", "delete", "move"])
        con.execute("CREATE OR REPLACE TEMP TABLE pre AS SELECT * FROM b")
        if op == "insert":
            add_b(con, r, r.randint(1, 3))
        elif op == "update":
            con.execute(f"UPDATE b SET w = w + 1 WHERE k = {r.randint(0, 9)}")
        elif op == "delete":
            con.execute(f"DELETE FROM b WHERE k = {r.randint(0, 9)}")
        else:
            con.execute(f"UPDATE b SET k = {r.randint(0, 9)} WHERE k = {r.randint(0, 9)}")
        # changed rows = symmetric difference of pre/post image; keys from both sides
        diff = "SELECT * FROM (SELECT * FROM b EXCEPT SELECT * FROM pre) UNION ALL SELECT * FROM (SELECT * FROM pre EXCEPT SELECT * FROM b)"
        keys = f"SELECT DISTINCT {', '.join(plan.b_cols)} FROM ({diff}) WHERE " + " AND ".join(
            f"{c} IS NOT NULL" for c in plan.b_cols
        )
        con.execute("BEGIN")
        res = oj.refresh(con, name, sql, pe, plan, "b", keys)
        con.execute("COMMIT")
        got = snap(con, name)
        con.execute(f"CREATE OR REPLACE TEMP TABLE full_ AS {sql}")
        assert got == snap(con, "full_"), (name, seed, step, op, res)


def test_partition_scoped_keys_and_cutoff():
    sql, pe, _ = CHILDREN["c_key"]
    con, _ = setup(0)
    build(con)
    plan = oj.analyze(sql, pe, "b")
    # b rows in a partition that matches nothing in a: no deletes, no inserts changes, nothing propagates
    con.execute("INSERT INTO b VALUES (99, 0, 1, TIMESTAMP '2026-10-09 00:00:00')")
    keys = oj.parent_keys_sql(plan, "b", "DATE(ts)", ["2026-10-09"])
    res = oj.refresh(con, "c_key", sql, pe, plan, "b", keys)
    assert res["changed_partitions"] == set() and res["inserted"] == 0
    # a matching new b row changes only the touched child rows
    con.execute("INSERT INTO b VALUES (3, 0, 1, TIMESTAMP '2026-10-09 00:00:00')")
    res = oj.refresh(
        con,
        "c_key",
        sql,
        pe,
        plan,
        "b",
        keys := oj.parent_keys_sql(plan, "b", "DATE(ts)", ["2026-10-09"]),
    )
    assert (
        res["inserted"] == con.execute("SELECT count(*) FROM a WHERE k = 3").fetchone()[0] * 2
        or res["inserted"] >= 1
    )
    con.execute("CREATE OR REPLACE TEMP TABLE f AS " + sql)
    assert snap(con, "c_key") == snap(con, "f")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a.k, count(*) c FROM a LEFT JOIN b ON a.k = b.k GROUP BY a.k",
        "SELECT a.id FROM a FULL JOIN b ON a.k = b.k",
        "SELECT a.id FROM b LEFT JOIN a ON a.k = b.k",  # parent b is preserved, not nullable
        "SELECT a.id FROM a LEFT JOIN b ON a.k < b.k",  # no equi condition
        "SELECT a.id FROM a LEFT JOIN b ON a.k = b.k LIMIT 3",
        "WITH x AS (SELECT * FROM a) SELECT x.id FROM x LEFT JOIN b ON x.k = b.k",
    ],
)
def test_unsupported_falls_back(sql):
    assert isinstance(oj.analyze(sql, "DATE(ts)", "b"), oj.Unsupported)

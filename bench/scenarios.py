"""Source-mutation scenarios. Each mutate(con, rng) edits the warehouse sources and returns the external signals
[(table, [partition values] | ALL)] that a data-landing system would send. Same seed => same mutation on both sides.

Row counts are fractions of the source so the scenarios scale with --sf.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

ALL = "ALL"  # signal for 'whole table / unknown extent'


@dataclass
class Scenario:
    name: str
    doc: str
    mutate: Callable
    requires: frozenset = field(default_factory=frozenset)


def _new_load_day(con, k=1):
    d = con.execute("SELECT max(CAST(lo_loaded_ts AS DATE)) FROM lineorder").fetchone()[0]
    return [d + __import__("datetime").timedelta(days=i + 1) for i in range(k)]


def _clone_rows(con, rng, n, day, order_dates=None, edits=""):
    """Insert n new lineorder rows cloned from random existing rows, landing on load date `day`."""
    base = con.execute("SELECT max(lo_rowid) FROM lineorder").fetchone()[0]
    tot = con.execute("SELECT count(*) FROM lineorder").fetchone()[0]
    ids = rng.sample(range(tot), min(n, tot))
    con.execute(
        "CREATE TEMP TABLE _src AS SELECT *, row_number() OVER (ORDER BY lo_rowid) - 1 AS rn FROM lineorder"
    )
    con.execute(f"""INSERT INTO lineorder
        SELECT {base} + row_number() OVER () AS lo_rowid, lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey,
               {order_dates or "lo_orderdate"} AS lo_orderdate,
               CAST(TIMESTAMP '{day} 00:00:00' AS TIMESTAMP) + INTERVAL (lo_orderkey % 24) HOUR AS lo_loaded_ts,
               lo_quantity, lo_extendedprice, lo_discount, lo_revenue, lo_supplycost
        FROM _src WHERE rn IN ({",".join(map(str, ids))})""")
    con.execute("DROP TABLE _src")


def noop(con, rng):
    return []


def resignal_identical(con, rng):
    ds = con.execute(
        "SELECT DISTINCT CAST(lo_loaded_ts AS DATE) FROM lineorder ORDER BY 1 DESC LIMIT 1"
    ).fetchone()[0]
    return [("lineorder", [str(ds)])]


def _latest_orderdates(con):
    return con.execute("SELECT max(lo_orderdate) FROM lineorder").fetchone()[0]


def new_day(con, rng, days=1, frac=0.002):
    n = con.execute("SELECT count(*) FROM lineorder").fetchone()[0]
    out = []
    for day in _new_load_day(con, days):
        # new rows carry the latest business dates (typical append), shifted to the 3 dates before the max
        _clone_rows(
            con,
            rng,
            max(int(n * frac / days), 20),
            day,
            order_dates=f"(SELECT max(lo_orderdate) FROM lineorder WHERE lo_loaded_ts < TIMESTAMP '{day}') - CAST(lo_orderkey % 3 AS INT)",
        )
        out.append(str(day))
    return [("lineorder", out)]


def late_update(con, rng, frac=0.001, recent_days=None):
    """Existing rows are corrected and re-delivered on a new load day. recent_days=None: rows from anywhere in
    history (scattered); recent_days=N: only rows whose business date is within N days of the newest."""
    n = con.execute("SELECT count(*) FROM lineorder").fetchone()[0]
    (day,) = _new_load_day(con, 1)
    k = max(int(n * frac), 10)
    where = (
        f"WHERE lo_orderdate > (SELECT max(lo_orderdate) FROM lineorder) - INTERVAL {recent_days} DAY"
        if recent_days
        else ""
    )
    ids = [
        r[0]
        for r in con.execute(
            f"SELECT lo_rowid FROM (SELECT lo_rowid FROM lineorder {where}) USING SAMPLE {k} ROWS (reservoir, {rng.randrange(10**6)})"
        ).fetchall()
    ]
    lst = ",".join(map(str, ids))
    con.execute(
        f"UPDATE lineorder SET lo_revenue = lo_revenue + 1000, lo_loaded_ts = TIMESTAMP '{day} 06:00:00' WHERE lo_rowid IN ({lst})"
    )
    return [("lineorder", [str(day)])]


def dim_change(con, rng, k=5):
    ids = [
        r[0] for r in con.execute("SELECT c_custkey FROM customer ORDER BY c_custkey").fetchall()
    ]
    pick = rng.sample(ids, min(k, len(ids)))
    con.execute(
        f"UPDATE customer SET c_region = 'ASIA', c_nation = 'CHINA', c_city = 'CHINA0' WHERE c_custkey IN ({','.join(map(str, pick))})"
    )
    return [("customer", ALL)]


def source_column_added(con, rng):
    con.execute("ALTER TABLE lineorder ADD COLUMN lo_note VARCHAR")
    return [("lineorder", ALL)]


def edge_mix(con, rng):
    """Correctness probe: NULL measures, an unmatched dimension key, rows that move business date, rows that
    leave the staging filter (quantity <= 0), and a late update, all in one delivery."""
    (day,) = _new_load_day(con, 1)
    _clone_rows(con, rng, 30, day, order_dates="(SELECT max(lo_orderdate) FROM lineorder)")
    con.execute(
        f"UPDATE lineorder SET lo_revenue = NULL, lo_discount = NULL WHERE lo_loaded_ts >= TIMESTAMP '{day}' AND lo_orderkey % 5 = 0"
    )
    con.execute(
        f"UPDATE lineorder SET lo_custkey = 99999999 WHERE lo_loaded_ts >= TIMESTAMP '{day}' AND lo_orderkey % 7 = 0"
    )
    old = [
        r[0]
        for r in con.execute(
            f"SELECT lo_rowid FROM lineorder WHERE lo_loaded_ts < TIMESTAMP '{day}' USING SAMPLE 60 ROWS (reservoir, {rng.randrange(10**6)})"
        ).fetchall()
    ]
    a, b, c = old[:20], old[20:40], old[40:]
    ts = f"lo_loaded_ts = TIMESTAMP '{day} 07:00:00'"
    con.execute(
        f"UPDATE lineorder SET lo_orderdate = lo_orderdate + 5 * INTERVAL 1 DAY, {ts} WHERE lo_rowid IN ({','.join(map(str, a))}) AND lo_orderdate < DATE '1998-12-20'"
    )
    con.execute(
        f"UPDATE lineorder SET lo_quantity = 0, {ts} WHERE lo_rowid IN ({','.join(map(str, b))})"
    )
    con.execute(
        f"UPDATE lineorder SET lo_extendedprice = lo_extendedprice * 2, {ts} WHERE lo_rowid IN ({','.join(map(str, c))})"
    )
    return [("lineorder", [str(day)])]


def model_sql_change(con, rng):
    raise NotImplementedError  # needs a candidate that re-plans after a repo change; see README "Schema/model changes"


SCENARIOS = {
    s.name: s
    for s in [
        Scenario("unchanged_no_signal", "cron wake-up, nothing arrived", noop),
        Scenario(
            "unchanged_resignal",
            "latest partition re-delivered with identical data",
            resignal_identical,
        ),
        Scenario("new_day", "one new load day (0.2% of rows) appended", new_day),
        Scenario(
            "backfill_7_days", "seven load days arrive at once", lambda c, r: new_day(c, r, days=7)
        ),
        Scenario(
            "late_updates_recent",
            "0.1% of rows from the last 14 business days corrected, re-delivered",
            lambda c, r: late_update(c, r, recent_days=14),
        ),
        Scenario(
            "late_updates_scattered",
            "0.1% of rows from anywhere in 7 years corrected, re-delivered",
            late_update,
        ),
        Scenario("dimension_change", "5 customers move region", dim_change),
        Scenario(
            "source_column_added",
            "source gains an unused column; signalled ALL",
            source_column_added,
        ),
        Scenario(
            "edge_mix",
            "NULLs, unmatched join key, moved dates, filtered-out rows, late update",
            edge_mix,
        ),
        Scenario(
            "model_sql_change",
            "a model's SQL gains a column (not supported yet)",
            model_sql_change,
            frozenset({"model_change"}),
        ),
    ]
}

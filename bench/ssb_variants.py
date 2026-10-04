"""Deterministic, offline-only perturbations of freshly loaded SSB sources."""

import math

VARIANTS = (
    "clean",
    "nulls",
    "duplicates",
    "unmatched_keys",
    "empty_partitions",
    "skewed_keys",
    "ties",
)
DIMENSIONS = (
    ("customer", "c_custkey"),
    ("supplier", "s_suppkey"),
    ("part", "p_partkey"),
    ("dates", "d_datekey"),
)


def _selected(table, key):
    # Stable selection independent of physical insertion order and DuckDB hash versions.
    return f"SELECT {key} FROM {table} ORDER BY md5(CAST({key} AS VARCHAR) || ?), {key} LIMIT ?"


def _update(con, table, key, column, value, count, seed, salt):
    con.execute(
        f"UPDATE {table} SET {column} = ? WHERE {key} IN ({_selected(table, key)})",
        [value, f"{seed}:{salt}", count],
    )


def apply_variant(con, name, seed):
    """Mutate a caller-owned DuckDB source connection, atomically, without generating data.

    Use a fresh load for each variant/seed, before building derived models. Non-clean
    variants require >=10 facts with unique non-NULL lo_rowid and nonempty dimensions
    with unique non-NULL keys. Skew requires >=4 customers; empty_partitions requires
    >=2 non-NULL business dates. No active caller transaction is supported.
    """
    if name not in VARIANTS:
        raise ValueError(f"unknown SSB variant: {name!r}; choose from {', '.join(VARIANTS)}")
    if type(seed) is not int:
        raise ValueError("seed must be an integer (not bool)")
    if name == "clean":
        return

    # Begin outside try: failure to begin must not roll back a caller's transaction.
    con.execute("BEGIN TRANSACTION")
    try:
        count, unique, present = con.execute(
            "SELECT count(*), count(DISTINCT lo_rowid), count(lo_rowid) FROM lineorder"
        ).fetchone()
        if count < 10 or unique != count or present != count:
            raise ValueError("variants require >=10 facts with unique non-NULL lo_rowid")
        dimensions = {}
        for table, key in DIMENSIONS:
            n, distinct, present = con.execute(
                f"SELECT count(*), count(DISTINCT {key}), count({key}) FROM {table}"
            ).fetchone()
            if not n or distinct != n or present != n:
                raise ValueError(f"fresh {table} requires nonempty unique non-NULL {key}")
            dimensions[table] = n
        sample = math.ceil(count / 5)

        if name == "nulls":
            for column in (
                "lo_quantity",
                "lo_extendedprice",
                "lo_discount",
                "lo_revenue",
                "lo_supplycost",
            ):
                _update(con, "lineorder", "lo_rowid", column, None, sample, seed, column)
            for table, key, columns in (
                ("customer", "c_custkey", ("c_name", "c_region")),
                ("supplier", "s_suppkey", ("s_name", "s_region")),
                ("part", "p_partkey", ("p_name", "p_brand1")),
            ):
                for column in columns:
                    _update(
                        con,
                        table,
                        key,
                        column,
                        None,
                        math.ceil(dimensions[table] / 5),
                        seed,
                        column,
                    )

        elif name == "duplicates":
            for table, key in DIMENSIONS:
                con.execute(
                    f"INSERT INTO {table} SELECT * FROM {table} WHERE {key} IN ({_selected(table, key)})",
                    [f"{seed}:{table}", math.ceil(dimensions[table] / 10)],
                )
            maximum = con.execute("SELECT max(lo_rowid) FROM lineorder").fetchone()[0]
            con.execute(
                "INSERT INTO lineorder SELECT * REPLACE "
                "(? + row_number() OVER (ORDER BY lo_rowid) AS lo_rowid) FROM lineorder "
                f"WHERE lo_rowid IN ({_selected('lineorder', 'lo_rowid')})",
                [maximum, f"{seed}:facts", math.ceil(count / 10)],
            )

        elif name == "unmatched_keys":
            for column, table, key in (
                ("lo_custkey", "customer", "c_custkey"),
                ("lo_suppkey", "supplier", "s_suppkey"),
                ("lo_partkey", "part", "p_partkey"),
                ("lo_orderdate", "dates", "d_date"),
            ):
                absent = con.execute(f"SELECT min({key}) - 1 FROM {table}").fetchone()[0]
                _update(con, "lineorder", "lo_rowid", column, absent, sample, seed, column)

        elif name == "empty_partitions":
            n = con.execute("SELECT count(DISTINCT lo_orderdate) FROM lineorder").fetchone()[0]
            if n < 2:
                raise ValueError("empty_partitions requires >=2 non-NULL business dates")
            con.execute(
                "DELETE FROM lineorder WHERE lo_orderdate IN (SELECT lo_orderdate FROM "
                "(SELECT DISTINCT lo_orderdate FROM lineorder WHERE lo_orderdate IS NOT NULL) "
                "ORDER BY md5(CAST(lo_orderdate AS VARCHAR) || ?), lo_orderdate LIMIT ?)",
                [f"{seed}:days", min(n - 1, math.ceil(n / 5))],
            )
            con.execute("DELETE FROM supplier")

        elif name == "skewed_keys":
            if dimensions["customer"] < 4:
                raise ValueError("skewed_keys requires >=4 customers")
            keys = [
                r[0]
                for r in con.execute(
                    _selected("customer", "c_custkey"), [f"{seed}:customers", 4]
                ).fetchall()
            ]
            # Exactly ceil(90% * N) on three keys, remainder on a fourth.
            con.execute(
                "UPDATE lineorder SET lo_custkey = ranks.new_key FROM ("
                "SELECT lo_rowid, CASE WHEN rn <= ? THEN CASE (rn - 1) % 3 "
                "WHEN 0 THEN ? WHEN 1 THEN ? ELSE ? END ELSE ? END AS new_key FROM "
                "(SELECT lo_rowid, row_number() OVER "
                "(ORDER BY md5(CAST(lo_rowid AS VARCHAR) || ?), lo_rowid) AS rn FROM lineorder)"
                ") AS ranks WHERE lineorder.lo_rowid = ranks.lo_rowid",
                [math.ceil(count * 9 / 10), *keys, f"{seed}:skew"],
            )

        elif name == "ties":
            day, stamp = con.execute(
                "SELECT lo_orderdate, lo_loaded_ts FROM lineorder WHERE lo_rowid IN "
                f"({_selected('lineorder', 'lo_rowid')})",
                [f"{seed}:anchor", 1],
            ).fetchone()
            for column, value in (("lo_orderdate", day), ("lo_loaded_ts", stamp)):
                _update(
                    con, "lineorder", "lo_rowid", column, value, math.ceil(count / 2), seed, "ties"
                )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise

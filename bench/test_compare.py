"""Offline tests for bench/compare.py. Skipped unless duckdb (the `bench` extra) is installed.

Every table here is a handful of synthetic rows, so no generator, network or credentials are needed.
Run them with `python -m pytest bench -q`.
"""

import pytest

pytest.importorskip("duckdb")

import duckdb  # noqa: E402

from bench import compare  # noqa: E402


def _pair(statements_a, statements_b):
    """Two independent in-memory connections holding the same table name `t`."""
    ca, cb = duckdb.connect(), duckdb.connect()
    for sql in statements_a:
        ca.execute(sql)
    for sql in statements_b:
        cb.execute(sql)
    return ca, cb


def _tables(sql_a, sql_b, name="t"):
    return _pair([f"CREATE TABLE {name} AS {sql_a}"], [f"CREATE TABLE {name} AS {sql_b}"])


def _attached(path):
    """A warehouse handle shaped like `bench/harness.py`'s: an attached db that is in use."""
    con = duckdb.connect()
    con.execute(f"ATTACH '{path}' AS w")
    con.execute("USE w")
    return con


def _rows(*rows: tuple) -> str:
    """A two-column (k, v) fixture table from short tuples: _rows((1, 'x'), (2, None))."""
    return " UNION ALL ".join(
        "SELECT "
        + ", ".join(f"{_lit(value)} AS {name}" for value, name in zip(row, ("k", "v"), strict=True))
        for row in rows
    )


def _lit(value) -> str:
    if value is None:
        return "NULL"
    return f"'{value}'" if isinstance(value, str) else repr(value)


# --------------------------------------------------------------------------- multiset equality


def test_identical_rows_in_different_order_are_equal():
    a = "SELECT 1 AS k, 'x' AS v UNION ALL SELECT 2 AS k, 'y' AS v"
    b = "SELECT 2 AS k, 'y' AS v UNION ALL SELECT 1 AS k, 'x' AS v"
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t") == []


def test_duplicate_count_difference_is_reported():
    a = "SELECT 1 AS k UNION ALL SELECT 1 AS k UNION ALL SELECT 2 AS k"
    b = "SELECT 1 AS k UNION ALL SELECT 2 AS k"
    ca, cb = _tables(a, b)
    problems = compare.compare_tables(ca, cb, "t")
    assert problems
    assert any("only in a" in p for p in problems)
    assert any("row counts differ" in p for p in problems)


def test_null_equals_null_but_not_empty_string():
    ca, cb = _tables("SELECT NULL AS k", "SELECT NULL AS k")
    assert compare.compare_tables(ca, cb, "t") == []
    ca, cb = _tables("SELECT NULL AS k", "SELECT '' AS k")
    assert compare.compare_tables(ca, cb, "t") != []


def test_nulls_in_group_keys_compare_as_equal():
    a = _rows((None, 1), ("a", 2), ("a", 2))
    b = _rows(("a", 2), (None, 1), ("a", 2))
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t") == []


def test_empty_versus_empty_is_equal_and_one_empty_is_not():
    ca, cb = _tables("SELECT 1 AS k WHERE false", "SELECT 1 AS k WHERE false")
    assert compare.compare_tables(ca, cb, "t") == []
    ca, cb = _tables("SELECT 1 AS k", "SELECT 1 AS k WHERE false")
    problems = compare.compare_tables(ca, cb, "t")
    assert any("row counts differ" in p for p in problems)


def test_column_order_difference_is_reported():
    ca, cb = _tables("SELECT 1 AS k, 2 AS v", "SELECT 2 AS v, 1 AS k")
    problems = compare.compare_tables(ca, cb, "t")
    assert len(problems) == 1
    assert "columns differ" in problems[0]


def test_samples_are_capped_at_five():
    a = "SELECT i AS k FROM range(20) t(i)"
    b = "SELECT i + 100 AS k FROM range(20) t(i)"
    ca, cb = _tables(a, b)
    problems = compare.compare_tables(ca, cb, "t")
    assert len(problems) == 1 + compare.SAMPLE_LIMIT


# --------------------------------------------------------------------------- float tolerance


def test_float_noise_below_tolerance_is_equal():
    a = "SELECT 1 AS k, 100.0::DOUBLE AS v"
    b = "SELECT 1 AS k, (100.0 * (1 + 1e-12))::DOUBLE AS v"
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t", float_tol=1e-9) == []


def test_float_noise_above_tolerance_is_reported():
    a = "SELECT 1 AS k, 100.0::DOUBLE AS v"
    b = "SELECT 1 AS k, (100.0 * (1 + 1e-6))::DOUBLE AS v"
    ca, cb = _tables(a, b)
    problems = compare.compare_tables(ca, cb, "t", float_tol=1e-9)
    assert problems
    assert any("beyond float_tol" in p for p in problems)


def test_float_tol_zero_is_exact():
    a = "SELECT 1 AS k, (0.1::DOUBLE + 0.2::DOUBLE) AS v"
    b = "SELECT 1 AS k, 0.3::DOUBLE AS v"
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t", float_tol=1e-9) == []
    assert compare.compare_tables(ca, cb, "t", float_tol=0) != []


def test_nan_equals_nan_but_not_a_number():
    ca, cb = _tables(
        "SELECT 1 AS k, CAST('nan' AS DOUBLE) AS v", "SELECT 1 AS k, CAST('nan' AS DOUBLE) AS v"
    )
    assert compare.compare_tables(ca, cb, "t") == []
    ca, cb = _tables("SELECT 1 AS k, CAST('nan' AS DOUBLE) AS v", "SELECT 1 AS k, 1.0 AS v")
    assert compare.compare_tables(ca, cb, "t") != []


def test_infinity_compares_exactly():
    ca, cb = _tables(
        "SELECT 1 AS k, CAST('inf' AS DOUBLE) AS v", "SELECT 1 AS k, CAST('inf' AS DOUBLE) AS v"
    )
    assert compare.compare_tables(ca, cb, "t") == []
    ca, cb = _tables(
        "SELECT 1 AS k, CAST('inf' AS DOUBLE) AS v", "SELECT 1 AS k, CAST('-inf' AS DOUBLE) AS v"
    )
    assert compare.compare_tables(ca, cb, "t") != []


def test_float_duplicate_counts_still_matter():
    a = _rows((1, 1.0), (1, 1.0), (2, 2.0))
    b = _rows((1, 1.0), (2, 2.0))
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t") != []


def test_exact_types_are_not_treated_as_floats():
    a = "SELECT 1 AS k, DATE '2024-01-01' AS d, 1.5::DECIMAL(18,6) AS m"
    b = "SELECT 1 AS k, DATE '2024-01-01' AS d, 1.5::DECIMAL(18,6) AS m"
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t") == []
    b2 = "SELECT 1 AS k, DATE '2024-01-02' AS d, 1.5::DECIMAL(18,6) AS m"
    ca, cb = _tables(a, b2)
    assert compare.compare_tables(ca, cb, "t") != []


def test_greedy_match_does_not_hide_a_value_from_another_row():
    """1.0 matches 1.0000000001, so 5.0 has nothing left to pair with and 9.0 must be reported."""
    a = "SELECT 1 AS k, 1.0::DOUBLE AS v UNION ALL SELECT 1 AS k, 5.0::DOUBLE AS v"
    b = "SELECT 1 AS k, 1.0000000001::DOUBLE AS v UNION ALL SELECT 1 AS k, 9.0::DOUBLE AS v"
    ca, cb = _tables(a, b)
    problems = compare.compare_tables(ca, cb, "t", float_tol=1e-9)
    assert problems
    assert any("beyond float_tol" in p for p in problems)


def test_extra_row_inside_a_matched_key_is_still_reported():
    a = "SELECT 1 AS k, 1.0::DOUBLE AS v"
    b = "SELECT 1 AS k, 1.0::DOUBLE AS v UNION ALL SELECT 1 AS k, 9.0::DOUBLE AS v"
    ca, cb = _tables(a, b)
    problems = compare.compare_tables(ca, cb, "t", float_tol=1e-9)
    assert problems
    assert any("only in b" in p for p in problems)


# --------------------------------------------------------------------------- ordering


def test_order_ties_are_not_a_difference():
    a = _rows((1, "x"), (1, "y"), (2, "z"))
    b = _rows((1, "y"), (1, "x"), (2, "z"))
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t") == []
    assert compare.compare_tables(ca, cb, "t", ordered_by=["k"]) == []


def test_real_order_difference_is_not_a_difference_for_two_tables():
    """Both sides are sorted the same way, so relations compare as multisets.

    `ordered_by` only changes the verdict for a caller that materialises an ordered result set; the
    checker never reports an ordering tie.
    """
    a = _rows((1, "x"), (2, "y"))
    b = _rows((2, "y"), (1, "x"))
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t") == []
    assert compare.compare_tables(ca, cb, "t", ordered_by=["k"]) == []


def test_ordering_does_not_hide_a_real_difference():
    a = _rows((1, "x"), (2, "y"))
    b = _rows((1, "x"), (3, "y"))
    ca, cb = _tables(a, b)
    assert compare.compare_tables(ca, cb, "t", ordered_by=["k"]) != []


def test_unknown_ordered_by_column_is_reported():
    ca, cb = _tables("SELECT 1 AS k", "SELECT 1 AS k")
    problems = compare.compare_tables(ca, cb, "t", ordered_by=["nope"])
    assert problems and "ordered_by" in problems[0]


# --------------------------------------------------------------------------- paths and errors


CASES = [
    (
        "SELECT 1 AS k, 'a' AS v UNION ALL SELECT 2 AS k, 'b' AS v",
        "SELECT 2 AS k, 'b' AS v UNION ALL SELECT 1 AS k, 'a' AS v",
    ),
    ("SELECT 1 AS k, 'a' AS v UNION ALL SELECT 1 AS k, 'a' AS v", "SELECT 1 AS k, 'a' AS v"),
    ("SELECT NULL AS k, 1.0 AS v", "SELECT NULL AS k, 1.0 + 1e-12 AS v"),
    ("SELECT 1 AS k, 1.0 AS v", "SELECT 1 AS k, 2.0 AS v"),
    (
        "SELECT 1 AS d, 9.0 AS v UNION ALL SELECT 1 AS d, 8.0 AS v",
        "SELECT 1 AS d, 8.0 AS v UNION ALL SELECT 1 AS d, 9.0 AS v",
    ),
]


@pytest.mark.parametrize("sql_a,sql_b", CASES)
@pytest.mark.parametrize("order_mode", ["unordered", "by_first_column", "by_all_columns"])
def test_same_connection_sql_path_matches_the_streaming_path(sql_a, sql_b, order_mode):
    con = duckdb.connect()
    con.execute(f"CREATE TABLE a AS {sql_a}")
    con.execute(f"CREATE TABLE b AS {sql_b}")
    cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM a").fetchall()]
    ordered = {"unordered": None, "by_first_column": cols[:1], "by_all_columns": cols}[order_mode]
    streaming = compare.compare_tables(con.cursor(), con.cursor(), ("a", "b"), ordered_by=ordered)
    sql = compare.compare_tables(con, con, ("a", "b"), ordered_by=ordered)
    # Both paths must reach the same verdict: equal in the same cases, unequal in the same cases.
    assert (streaming == []) == (sql == [])


def test_same_connection_pair_of_names_is_supported():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE base AS SELECT 1 AS k UNION ALL SELECT 1 AS k UNION ALL SELECT 2 AS k"
    )
    con.execute("CREATE TABLE cand AS SELECT 1 AS k UNION ALL SELECT 2 AS k")
    problems = compare.compare_tables(con, con, ("base", "cand"))
    assert problems and "base vs cand" in problems[0]


def test_same_connection_float_tolerance_still_compares():
    con = duckdb.connect()
    con.execute("CREATE TABLE a AS SELECT 1 AS k, 100.0::DOUBLE AS v")
    con.execute("CREATE TABLE b AS SELECT 1 AS k, 100.0::DOUBLE + 1e-9 AS v")
    assert compare.compare_tables(con, con, ("a", "b")) == []
    con.execute("CREATE TABLE c AS SELECT 1 AS k, 101.0::DOUBLE AS v")
    assert compare.compare_tables(con, con, ("a", "c")) != []


def test_connections_search_path_is_respected(tmp_path):
    """The harness does `ATTACH ... AS w; USE w`, so the cursors need the same search path."""
    ca = _attached(str(tmp_path / "a.duckdb"))
    cb = _attached(str(tmp_path / "b.duckdb"))
    try:
        for con in (ca, cb):
            con.execute("CREATE TABLE t AS SELECT 1 AS k, 'x' AS v")
        assert compare.compare_tables(ca, cb, "t") == []
        cb.execute("CREATE OR REPLACE TABLE t AS SELECT 2 AS k, 'y' AS v")
        assert compare.compare_tables(ca, cb, "t") != []
    finally:
        ca.close()
        cb.close()


def test_same_connection_tolerance_with_an_attached_database(tmp_path):
    con = _attached(str(tmp_path / "w.duckdb"))
    try:
        con.execute("CREATE TABLE u AS SELECT 1 AS k, 100.0::DOUBLE AS v")
        con.execute("CREATE TABLE v AS SELECT 1 AS k, 100.0::DOUBLE + 1e-9 AS v")
        assert compare.compare_tables(con, con, ("u", "v")) == []
    finally:
        con.close()


def test_missing_table_is_reported_not_raised():
    ca, cb = _tables("SELECT 1 AS k", "SELECT 1 AS k")
    problems = compare.compare_tables(ca, cb, "absent")
    assert problems and "cannot read table" in problems[0]


def test_non_identifier_table_name_is_refused():
    ca, cb = _tables("SELECT 1 AS k", "SELECT 1 AS k")
    problems = compare.compare_tables(ca, cb, "t; DROP TABLE t")
    assert problems and "plain table identifier" in problems[0]


def test_negative_float_tol_is_rejected():
    ca, cb = _tables("SELECT 1 AS k", "SELECT 1 AS k")
    with pytest.raises(ValueError):
        compare.compare_tables(ca, cb, "t", float_tol=-1)


# --------------------------------------------------------------------------- compare_dag


def test_compare_dag_reports_per_table_results():
    ca, cb = _tables(
        "SELECT 1 AS k UNION ALL SELECT 1 AS k UNION ALL SELECT 2 AS k",
        "SELECT 1 AS k UNION ALL SELECT 2 AS k",
    )
    ca.execute("CREATE TABLE same AS SELECT 1 AS k")
    cb.execute("CREATE TABLE same AS SELECT 1 AS k")
    results = compare.compare_dag(ca, cb, ["same", "t"])
    assert set(results) == {"same", "t"}
    assert results["same"] == []
    assert results["t"]


def test_compare_dag_accepts_a_mapping_and_pairs():
    a = "SELECT 1 AS d, 2.0 AS v"
    ca, cb = _tables(a, a)
    assert compare.compare_dag(ca, cb, {"t": ["d"]}) == {"t": []}
    ca, cb = _tables(a, "SELECT 1 AS d, 3.0 AS v")
    assert compare.compare_dag(ca, cb, [("t", ["d"])])["t"]


def test_compare_dag_forwards_keyword_arguments():
    a = "SELECT 1 AS k, (0.1::DOUBLE + 0.2::DOUBLE) AS v"
    ca, cb = _tables(a, "SELECT 1 AS k, 0.3::DOUBLE AS v")
    assert compare.compare_dag(ca, cb, ["t"], float_tol=0)["t"]
    assert compare.compare_dag(ca, cb, ["t"], float_tol=1e-9)["t"] == []

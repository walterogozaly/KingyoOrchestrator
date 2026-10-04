"""Synthetic SSB-format sources exercise loader compatibility without BenchBox."""

import socket
from datetime import date, timedelta

import pytest

duckdb = pytest.importorskip("duckdb")

from bench import ssb_data  # noqa: E402
from bench.ssb_variants import DIMENSIONS, VARIANTS, apply_variant  # noqa: E402
from bench.variants import main  # noqa: E402


@pytest.fixture(scope="module")
def source_files(tmp_path_factory):
    path = tmp_path_factory.mktemp("ssb_variant_sources")
    path.joinpath("customer.tbl").write_text(
        "".join(
            f"{i}|Customer_{i}|addr_{i}|city_{i}|nation_{i}|region_{i}|phone_{i}|segment_{i}\n"
            for i in range(1, 13)
        ),
        encoding="utf-8",
    )
    path.joinpath("supplier.tbl").write_text(
        "".join(
            f"{i}|Supplier_{i}|addr_{i}|city_{i}|nation_{i}|region_{i}|phone_{i}\n"
            for i in range(1, 13)
        ),
        encoding="utf-8",
    )
    path.joinpath("part.tbl").write_text(
        "".join(
            f"{i}|Part_{i}|maker_{i}|category_{i}|brand_{i}|color_{i}|type_{i}|{i}|box\n"
            for i in range(1, 13)
        ),
        encoding="utf-8",
    )
    days = [date(1998, 12, 1) + timedelta(days=i) for i in range(20)]
    path.joinpath("date.tbl").write_text(
        "".join(
            f"{d:%Y%m%d}|{d:%Y-%m-%d}|Mon|December|{d.year}|{d:%Y%m}|Dec1998|1|{d.day}|{d.day}|12|49|Winter|0|0|0|1\n"
            for d in days
        ),
        encoding="utf-8",
    )
    path.joinpath("lineorder.tbl").write_text(
        "".join(
            f"{i}|1|{1 + i % 12}|{1 + i % 12}|{1 + i % 12}|{days[i % 20]:%Y%m%d}|URGENT|0|{i}|{i * 100}|9999|{i % 10}|{i * 90}|{i * 10}|2|{days[i % 20]:%Y%m%d}|AIR\n"
            for i in range(1, 101)
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError("variants must not contact a provider")

    monkeypatch.setattr(socket, "socket", deny)


@pytest.fixture
def con(source_files):
    connection = duckdb.connect()
    ssb_data.load_sources(connection, source_files)
    yield connection
    connection.close()


def snapshot(con):
    return {
        table: sorted(map(repr, con.execute(f"SELECT * FROM {table}").fetchall()))
        for table in ["lineorder", *(t for t, _ in DIMENSIONS)]
    }


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def test_nulls_create_measure_discount_and_attribute_nulls(con):
    apply_variant(con, "nulls", 42)
    for column in ["lo_quantity", "lo_extendedprice", "lo_discount", "lo_revenue", "lo_supplycost"]:
        assert scalar(con, f"SELECT count(*) FROM lineorder WHERE {column} IS NULL") == 20
    for table, columns in [
        ("customer", ["c_name", "c_region"]),
        ("supplier", ["s_name", "s_region"]),
        ("part", ["p_name", "p_brand1"]),
    ]:
        for column in columns:
            assert scalar(con, f"SELECT count(*) FROM {table} WHERE {column} IS NULL") == 3
    assert scalar(con, "SELECT count(DISTINCT lo_rowid) FROM lineorder") == 100


def test_duplicates_keep_unique_rowids_but_duplicate_business_rows(con):
    original = scalar(con, "SELECT max(lo_rowid) FROM lineorder")
    apply_variant(con, "duplicates", 42)
    assert scalar(con, "SELECT count(*) FROM lineorder") == 110
    assert scalar(con, "SELECT count(DISTINCT lo_rowid) FROM lineorder") == 110
    assert scalar(con, f"SELECT count(*) FROM lineorder WHERE lo_rowid > {original}") == 10
    assert (
        scalar(con, "SELECT count(*) FROM (SELECT DISTINCT * EXCLUDE(lo_rowid) FROM lineorder)")
        == 100
    )
    for table, key in DIMENSIONS:
        assert scalar(con, f"SELECT count(*) - count(DISTINCT {key}) FROM {table}") == 2


def test_unmatched_keys_break_each_join_for_at_least_twenty_percent(con):
    apply_variant(con, "unmatched_keys", 42)
    for column, table, key in [
        ("lo_custkey", "customer", "c_custkey"),
        ("lo_suppkey", "supplier", "s_suppkey"),
        ("lo_partkey", "part", "p_partkey"),
        ("lo_orderdate", "dates", "d_date"),
    ]:
        assert (
            scalar(
                con,
                f"SELECT count(*) FROM lineorder f WHERE NOT EXISTS "
                f"(SELECT 1 FROM {table} d WHERE d.{key} = f.{column})",
            )
            == 20
        )


def test_empty_partitions_remove_entire_days_and_empty_a_dimension(con):
    original_days = set(con.execute("SELECT DISTINCT lo_orderdate FROM lineorder").fetchall())
    apply_variant(con, "empty_partitions", 42)
    remaining = set(con.execute("SELECT DISTINCT lo_orderdate FROM lineorder").fetchall())
    assert len(original_days - remaining) == 4
    assert remaining < original_days
    assert scalar(con, "SELECT count(*) FROM lineorder") == 80
    assert scalar(con, "SELECT count(*) FROM supplier") == 0


def test_skew_places_ninety_percent_on_three_existing_customers(con):
    apply_variant(con, "skewed_keys", 42)
    counts = con.execute(
        "SELECT count(*) FROM lineorder GROUP BY lo_custkey ORDER BY count(*) DESC"
    ).fetchall()
    assert counts == [(30,), (30,), (30,), (10,)]
    assert (
        scalar(
            con,
            "SELECT count(*) FROM lineorder f WHERE NOT EXISTS "
            "(SELECT 1 FROM customer c WHERE c.c_custkey = f.lo_custkey)",
        )
        == 0
    )


def test_ties_give_half_of_rows_identical_sort_keys(con):
    apply_variant(con, "ties", 42)
    assert (
        scalar(
            con,
            "SELECT max(n) FROM (SELECT count(*) n FROM lineorder "
            "GROUP BY lo_orderdate, lo_loaded_ts)",
        )
        >= 50
    )
    assert scalar(con, "SELECT count(DISTINCT lo_rowid) FROM lineorder") == 100


@pytest.mark.parametrize("name", VARIANTS)
def test_same_seed_is_reproducible_and_other_seed_changes_nonclean(source_files, name):
    outcomes = []
    for seed in [42, 42, 43]:
        with duckdb.connect() as connection:
            ssb_data.load_sources(connection, source_files)
            before = snapshot(connection)
            apply_variant(connection, name, seed)
            after = snapshot(connection)
            if name == "clean":
                assert before == after
            else:
                assert before != after
            outcomes.append(after)
    assert outcomes[0] == outcomes[1]
    assert (outcomes[0] == outcomes[2]) == (name == "clean")


@pytest.mark.parametrize("name", VARIANTS)
def test_selection_does_not_depend_on_physical_row_order(source_files, name):
    outcomes = []
    for reverse in [False, True]:
        with duckdb.connect() as connection:
            ssb_data.load_sources(connection, source_files)
            if reverse:
                for table, key in [("lineorder", "lo_rowid"), *DIMENSIONS]:
                    connection.execute(
                        f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table} ORDER BY {key} DESC"
                    )
            apply_variant(connection, name, -7)
            outcomes.append(snapshot(connection))
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize("name,seed", [("missing", 0), ("nulls", True), ("nulls", "42")])
def test_bad_arguments_leave_sources_untouched(con, name, seed):
    before = snapshot(con)
    with pytest.raises(ValueError):
        apply_variant(con, name, seed)
    assert snapshot(con) == before


@pytest.mark.parametrize(
    "sql,name,reason",
    [
        ("DELETE FROM lineorder WHERE lo_rowid > 100", "nulls", ">=10 facts"),
        ("UPDATE lineorder SET lo_rowid = 1", "nulls", "unique non-NULL"),
        ("DELETE FROM customer WHERE c_custkey > 3", "skewed_keys", ">=4 customers"),
        (
            "UPDATE lineorder SET lo_orderdate = DATE '1998-12-01'",
            "empty_partitions",
            ">=2 non-NULL",
        ),
    ],
)
def test_unsupported_shapes_reject_atomically(con, sql, name, reason):
    con.execute(sql)
    before = snapshot(con)
    with pytest.raises(ValueError, match=reason):
        apply_variant(con, name, 42)
    assert snapshot(con) == before


def test_schema_failure_after_some_updates_rolls_back_all_updates(con):
    con.execute("ALTER TABLE part DROP COLUMN p_brand1")
    before = snapshot(con)
    with pytest.raises(duckdb.BinderException):
        apply_variant(con, "nulls", 42)
    assert snapshot(con) == before


def test_nested_transaction_failure_does_not_rollback_callers_work(con):
    con.execute("BEGIN")
    con.execute("UPDATE lineorder SET lo_revenue = 0")
    with pytest.raises(duckdb.TransactionException):
        apply_variant(con, "nulls", 42)
    # DuckDB marks the existing transaction aborted; only the caller should roll it back.
    con.execute("ROLLBACK")
    assert scalar(con, "SELECT min(lo_revenue) FROM lineorder") > 0


def test_listing_names(capsys):
    main(["--list"])
    assert capsys.readouterr().out.splitlines() == list(VARIANTS)

"""Offline regression witnesses for the partition-resolution audit (issues #25/#37).

Non-UTC day boundaries are now refused by the required PartitionConfig declaration.
The original synthetic rows retain the off-by-one-day precondition on both paths.
UTC controls execute Kingyo's own discovery SQL in DuckDB, with only these
mechanical adaptations: strip backticks, quote DAY, and install a TIMESTAMP_TRUNC
macro accepting only DAY/UTC and mapping it to date_trunc('day', ...).
No cloud client, credentials, or network are needed.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta

import pytest

from kingyo_orchestrator.adapters.bigquery_discovery import render_discovery_sql
from kingyo_orchestrator.core.partitions import (
    ChangeWindow,
    ColumnSpec,
    PartitionConfig,
    discovery_required,
    resolve_partitions,
)

duckdb = pytest.importorskip("duckdb")

# America/New_York is UTC-05:00 in January; a fixed offset keeps the witness
# deterministic and offline.
NEW_YORK_OFFSET = timedelta(hours=-5)

CONFIG = PartitionConfig(
    table="project_x.dataset_a.table_orders",
    change_column="last_upd_ts",
    partition_column="order_sold_ts",
    partition_time_zone="UTC",
    columns=(ColumnSpec("last_upd_ts", "TIMESTAMP"), ColumnSpec("order_sold_ts", "TIMESTAMP")),
)
# change column == partition column, so resolution never consults discovery results.
SAME_COLUMN_CONFIG = replace(CONFIG, change_column="order_sold_ts")

WINDOW = ChangeWindow(datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 5, 1, tzinfo=UTC))


@dataclass(frozen=True)
class Row:
    """One synthetic fact row, with the physical partition value its row would live in."""

    order_sold_ts: datetime
    last_upd_ts: datetime

    @property
    def utc_partition_day(self) -> date:
        return self.order_sold_ts.date()

    @property
    def physical_partition_day(self) -> date:
        """Partition day of a table partitioned by ``DATE(order_sold_ts, 'America/New_York')``."""
        return (self.order_sold_ts + NEW_YORK_OFFSET).date()


ROWS = (
    # Changed inside WINDOW, sold at 2026-01-02T02:00Z, which is still 2026-01-01 in New York.
    Row(datetime(2026, 1, 2, 2, tzinfo=UTC), datetime(2026, 1, 5, 0, 10, tzinfo=UTC)),
    # Unchanged control row in the same partition; it must not widen the answer.
    Row(datetime(2026, 1, 2, 20, tzinfo=UTC), datetime(2026, 1, 1, 3, tzinfo=UTC)),
)


def _physical_table(rows: tuple[Row, ...]) -> duckdb.DuckDBPyConnection:
    """Synthetic warehouse: Kingyo's three-part table name is catalog, schema and table."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    project, dataset, table = CONFIG.table.split(".")
    con.execute(f"ATTACH ':memory:' AS {project}")
    con.execute(f"CREATE SCHEMA {project}.{dataset}")
    columns = "order_sold_ts TIMESTAMP, last_upd_ts TIMESTAMP"
    con.execute(f"CREATE TABLE {project}.{dataset}.{table} ({columns})")
    con.executemany(
        f"INSERT INTO {project}.{dataset}.{table} VALUES (?, ?)",
        [(r.order_sold_ts.replace(tzinfo=None), r.last_upd_ts.replace(tzinfo=None)) for r in rows],
    )
    con.execute(
        "CREATE MACRO TIMESTAMP_TRUNC(ts_value, granularity, timezone_name) AS "
        "CASE WHEN granularity <> 'DAY' OR timezone_name <> 'UTC' "
        "THEN error('this witness only models DAY/UTC truncation') "
        "ELSE date_trunc('day', ts_value) END"
    )
    return con


def run_emitted_discovery(
    con: duckdb.DuckDBPyConnection, config: PartitionConfig, window: ChangeWindow
) -> list[datetime]:
    """Run the SQL `render_discovery_sql` produces; return what a BigQuery client hands back."""
    sql = (
        render_discovery_sql(config, window)
        .replace("`", "")
        .replace(", DAY, 'UTC')", ", 'DAY', 'UTC')")
    )
    return [row[0].replace(tzinfo=UTC) for row in con.execute(sql).fetchall()]


def changed_rows(rows: tuple[Row, ...], window: ChangeWindow) -> tuple[Row, ...]:
    """Ground truth: rows whose change timestamp falls in the half-open window.

    Computed in Python, not SQL, so the witness never depends on the query engine.
    """
    return tuple(r for r in rows if window.since <= r.last_upd_ts < window.until)


def test_discovery_refuses_non_utc_partition_day_boundary():
    """Reject the declared New York boundary before rendering a misleading UTC query."""
    rows = changed_rows(ROWS, WINDOW)
    assert rows[0].physical_partition_day == date(2026, 1, 1)
    assert rows[0].utc_partition_day == date(2026, 1, 2)
    assert discovery_required(CONFIG, WINDOW)
    with pytest.raises(ValueError, match=r"partition_time_zone.*UTC.*#37"):
        replace(CONFIG, partition_time_zone="America/New_York")


def test_same_column_short_circuit_refuses_non_utc_partition_day_boundary():
    """The window-derived path must refuse the same unsupported declaration."""
    changed = datetime(2026, 1, 5, 0, 30, tzinfo=UTC)
    row = Row(changed, changed)
    assert row.physical_partition_day == date(2026, 1, 4)
    assert row.utc_partition_day == date(2026, 1, 5)
    assert not discovery_required(SAME_COLUMN_CONFIG, WINDOW)
    with pytest.raises(ValueError, match=r"partition_time_zone.*UTC.*#37"):
        replace(SAME_COLUMN_CONFIG, partition_time_zone="America/New_York")


def test_utc_partitioned_table_is_unaffected_by_the_timezone_gap():
    """Control: a table partitioned on the UTC day resolves this same change correctly."""
    rows = (Row(datetime(2026, 1, 2, 2, tzinfo=UTC), datetime(2026, 1, 5, 0, 10, tzinfo=UTC)),)
    with _physical_table(rows) as con:
        discovered = run_emitted_discovery(con, CONFIG, WINDOW)
    selection = resolve_partitions(CONFIG, WINDOW, discovered)

    assert selection.partitions == (date(2026, 1, 2),)
    assert {r.utc_partition_day for r in rows} <= set(selection.partitions)


def test_emitted_discovery_sql_is_executable():
    """Guard the DuckDB adaptation: the emitted SQL runs here and returns the
    changed rows' UTC days.
    """
    with _physical_table(ROWS) as con:
        discovered = run_emitted_discovery(con, CONFIG, WINDOW)

    assert [value.isoformat() for value in discovered] == ["2026-01-02T00:00:00+00:00"]
    assert all(value.tzinfo is UTC for value in discovered), (
        "BigQuery hands back timezone-aware values"
    )


def test_witness_fixture_really_holds_the_changed_row():
    """Keep the witnesses honest: the fixture row is in the window, off by one from its UTC day."""
    rows = changed_rows(ROWS, WINDOW)
    assert len(rows) == 1
    assert rows[0].physical_partition_day == date(2026, 1, 1)
    assert rows[0].utc_partition_day == date(2026, 1, 2)

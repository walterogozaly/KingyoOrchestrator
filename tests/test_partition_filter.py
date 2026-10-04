import re
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from kingyo_orchestrator.adapters.bigquery_discovery import render_discovery_sql
from kingyo_orchestrator.adapters.bigquery_partition_filter import (
    MAX_PARTITION_DAYS,
    render_partition_filter,
)
from kingyo_orchestrator.core.partitions import (
    ChangeWindow,
    ColumnSpec,
    DayRange,
    PartitionConfig,
    PartitionSelection,
    UnsupportedPartitionError,
    resolve_partitions,
    select_partitions,
)

DATE_CONFIG = PartitionConfig(
    "project_x.dataset_a.table_orders",
    "last_upd_ts",
    "partition_date",
    (ColumnSpec("last_upd_ts", "TIMESTAMP"), ColumnSpec("partition_date", "DATE")),
)
TIMESTAMP_CONFIG = replace(
    DATE_CONFIG,
    columns=(ColumnSpec("last_upd_ts", "TIMESTAMP"), ColumnSpec("partition_date", "TIMESTAMP")),
)
WINDOW = ChangeWindow(datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 2, tzinfo=UTC))


def test_date_discovery_selects_raw_column_with_half_open_change_window():
    assert render_discovery_sql(DATE_CONFIG, WINDOW) == (
        "SELECT DISTINCT `partition_date` AS partition_start\n"
        "FROM `project_x.dataset_a.table_orders`\n"
        "WHERE `last_upd_ts` >= TIMESTAMP '2026-10-01 00:00:00.000000+00:00'\n"
        "  AND `last_upd_ts` < TIMESTAMP '2026-10-02 00:00:00.000000+00:00'"
    )


def test_date_discovery_results_include_late_partitions_and_fail_on_null():
    rows = [
        {"partition_start": date(2025, 1, 1)},
        {"partition_start": date(2025, 1, 2)},
        {"partition_start": date(2025, 1, 1)},
    ]
    selection = resolve_partitions(DATE_CONFIG, WINDOW, [row["partition_start"] for row in rows])
    assert selection.partitions == (date(2025, 1, 1), date(2025, 1, 2))
    with pytest.raises(UnsupportedPartitionError, match="NULL"):
        resolve_partitions(DATE_CONFIG, WINDOW, [None])


def test_date_filter_is_sorted_unique_literals_on_raw_alias_qualified_column():
    selection = PartitionSelection(
        (date(2026, 10, 3), date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 1)),
        (),
    )
    sql = render_partition_filter(DATE_CONFIG, selection, alias="T")
    assert sql == (
        "`T`.`partition_date` IN (\n"
        "  DATE '2026-10-01',\n"
        "  DATE '2026-10-02',\n"
        "  DATE '2026-10-03'\n)"
    )
    for forbidden in ("SELECT", "JOIN", "CAST(", "DATE(", "EXTRACT(", "TIMESTAMP_TRUNC("):
        assert forbidden not in sql


def _date_literal_days(sql: str) -> tuple[date, ...]:
    return tuple(date.fromisoformat(day) for day in re.findall(r"DATE '(\d{4}-\d{2}-\d{2})'", sql))


def _range_start_days(sql: str) -> tuple[date, ...]:
    starts = re.findall(r">= TIMESTAMP '(\d{4}-\d{2}-\d{2})", sql)
    return tuple(date.fromisoformat(day) for day in starts)


@pytest.mark.parametrize(
    "config,expected,emitted_days",
    [
        (
            DATE_CONFIG,
            "`partition_date` IN (\n  DATE '2026-10-01',\n  DATE '2026-10-03'\n)",
            _date_literal_days,
        ),
        (
            TIMESTAMP_CONFIG,
            "((`partition_date` >= TIMESTAMP '2026-10-01 00:00:00+00:00' "
            "AND `partition_date` < TIMESTAMP '2026-10-02 00:00:00+00:00') OR "
            "(`partition_date` >= TIMESTAMP '2026-10-03 00:00:00+00:00' "
            "AND `partition_date` < TIMESTAMP '2026-10-04 00:00:00+00:00'))",
            _range_start_days,
        ),
    ],
    ids=["date", "timestamp"],
)
def test_unqualified_filter_and_stale_ranges_do_not_fill_gaps(config, expected, emitted_days):
    selection = PartitionSelection(
        (date(2026, 10, 1), date(2026, 10, 3)),
        (DayRange(date(2026, 10, 1), date(2026, 10, 3)),),
    )
    sql = render_partition_filter(config, selection)
    assert sql == expected
    # One emitted literal or range start per selected day: a stale-range bug that bridged
    # the gap would collapse these into a single start, or add 2026-10-02 itself.
    assert emitted_days(sql) == selection.partitions


def test_timestamp_filter_merges_adjacent_days_but_preserves_gaps():
    selection = select_partitions([date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 4)])
    sql = render_partition_filter(TIMESTAMP_CONFIG, selection, alias="T")
    column = "`T`.`partition_date`"
    assert sql == (
        f"(({column} >= TIMESTAMP '2026-10-01 00:00:00+00:00' "
        f"AND {column} < TIMESTAMP '2026-10-03 00:00:00+00:00') OR "
        f"({column} >= TIMESTAMP '2026-10-04 00:00:00+00:00' "
        f"AND {column} < TIMESTAMP '2026-10-05 00:00:00+00:00'))"
    )
    for forbidden in ("SELECT", "JOIN", " IN ", "CAST(", "DATE(", "EXTRACT(", "TIMESTAMP_TRUNC("):
        assert forbidden not in sql


def test_timestamp_year_boundary_single_range():
    selection = select_partitions([date(2025, 12, 31)])
    assert render_partition_filter(TIMESTAMP_CONFIG, selection) == (
        "(`partition_date` >= TIMESTAMP '2025-12-31 00:00:00+00:00' "
        "AND `partition_date` < TIMESTAMP '2026-01-01 00:00:00+00:00')"
    )


@pytest.mark.parametrize("config", [DATE_CONFIG, TIMESTAMP_CONFIG])
def test_empty_selection_matches_nothing_even_with_stale_ranges(config):
    selection = PartitionSelection((), (DayRange(date(2026, 10, 1), date(2026, 10, 2)),))
    assert render_partition_filter(config, selection) == "FALSE"


@pytest.mark.parametrize("config", [DATE_CONFIG, TIMESTAMP_CONFIG])
def test_large_selection_is_explicitly_rejected(config):
    days = [date(2020, 1, 1) + timedelta(days=index) for index in range(MAX_PARTITION_DAYS + 1)]
    with pytest.raises(ValueError, match="at most 1000 unique days"):
        render_partition_filter(config, select_partitions(days))


def test_exactly_1000_date_literals_are_supported():
    days = [date(2020, 1, 1) + timedelta(days=index) for index in range(MAX_PARTITION_DAYS)]
    sql = render_partition_filter(DATE_CONFIG, select_partitions(days))
    assert sql.count("DATE '") == 1000


def test_limit_counts_unique_days_not_duplicate_input_values():
    sql = render_partition_filter(DATE_CONFIG, PartitionSelection((date(2026, 10, 1),) * 2000, ()))
    assert sql.count("DATE '") == 1


@pytest.mark.parametrize("alias", ["", "T.partition_date", "`T`", "T; SELECT 1", 42])
def test_alias_must_be_a_simple_identifier(alias):
    with pytest.raises(ValueError, match="alias"):
        render_partition_filter(DATE_CONFIG, select_partitions([date(2026, 10, 1)]), alias)


@pytest.mark.parametrize("value", [None, "2026-10-01", datetime(2026, 10, 1, tzinfo=UTC), True])
def test_invalid_day_values_fail_before_rendering(value):
    with pytest.raises(UnsupportedPartitionError, match="Python date"):
        render_partition_filter(DATE_CONFIG, PartitionSelection((value,), ()))


def test_date_max_is_literal_but_timestamp_max_has_no_representable_exclusive_bound():
    selection = select_partitions([date.max])
    assert render_partition_filter(DATE_CONFIG, selection) == (
        "`partition_date` IN (\n  DATE '9999-12-31'\n)"
    )
    with pytest.raises(ValueError, match="exclusive bound"):
        render_partition_filter(TIMESTAMP_CONFIG, selection)


def test_date_min_is_zero_padded_in_literal():
    assert "DATE '0001-01-01'" in render_partition_filter(
        DATE_CONFIG, select_partitions([date.min])
    )


def test_change_column_stays_timestamp_and_same_column_shortcut_still_works():
    with pytest.raises(ValueError, match="last_upd_ts.*TIMESTAMP"):
        replace(
            DATE_CONFIG,
            columns=(ColumnSpec("last_upd_ts", "DATE"), ColumnSpec("partition_date", "DATE")),
        )
    config = replace(TIMESTAMP_CONFIG, partition_column="last_upd_ts")
    selection = resolve_partitions(config, WINDOW)
    assert selection.partitions == (date(2026, 10, 1),)
    assert "TIMESTAMP '2026-10-02 00:00:00+00:00'" in render_partition_filter(config, selection)

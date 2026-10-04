from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from kingyo_orchestrator.adapters.bigquery_discovery import render_discovery_sql
from kingyo_orchestrator.core.partitions import (
    ChangeWindow,
    ColumnSpec,
    DayRange,
    PartitionConfig,
    PartitionSelection,
    UnsupportedPartitionError,
    discovery_required,
    resolve_partitions,
)

CONFIG = PartitionConfig(
    table="project_x.dataset_a.table_orders",
    change_column="last_upd_ts",
    partition_column="order_sold_ts",
    partition_time_zone="UTC",
    columns=(ColumnSpec("last_upd_ts", "TIMESTAMP"), ColumnSpec("order_sold_ts", "TIMESTAMP")),
)
SINCE = datetime(2026, 1, 1, tzinfo=UTC)
UNTIL = datetime(2026, 1, 2, tzinfo=UTC)
WINDOW = ChangeWindow(SINCE, UNTIL)


def fake_discovery_rows(rows, window):
    """Emulate caller-provided BigQuery results; no query is submitted."""
    result = []
    for row in rows:
        if window.since <= row["last_upd_ts"] < window.until:
            sold = row["order_sold_ts"]
            result.append(
                {
                    "partition_start": None
                    if sold is None
                    else sold.replace(hour=0, minute=0, second=0, microsecond=0)
                }
            )
    return result


def resolve_fake_rows(rows, window=WINDOW):
    discovered = fake_discovery_rows(rows, window)
    return resolve_partitions(CONFIG, window, (row["partition_start"] for row in discovered))


def test_sql_contract_has_utc_day_truncation_and_half_open_predicate():
    assert render_discovery_sql(CONFIG, WINDOW) == (
        "SELECT DISTINCT TIMESTAMP_TRUNC(`order_sold_ts`, DAY, 'UTC') AS partition_start\n"
        "FROM `project_x.dataset_a.table_orders`\n"
        "WHERE `last_upd_ts` >= TIMESTAMP '2026-01-01 00:00:00.000000+00:00'\n"
        "  AND `last_upd_ts` < TIMESTAMP '2026-01-02 00:00:00.000000+00:00'"
    )


def test_late_updates_use_old_partitions_not_change_window_dates():
    rows = [
        {"last_upd_ts": SINCE, "order_sold_ts": datetime(2025, 3, 2, 12, tzinfo=UTC)},
        {"last_upd_ts": SINCE, "order_sold_ts": datetime(2025, 3, 1, 9, tzinfo=UTC)},
        {"last_upd_ts": SINCE, "order_sold_ts": datetime(2025, 3, 4, tzinfo=UTC)},
        {"last_upd_ts": SINCE, "order_sold_ts": datetime(2025, 3, 2, 20, tzinfo=UTC)},
    ]
    assert resolve_fake_rows(rows) == PartitionSelection(
        (date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 4)),
        (
            DayRange(date(2025, 3, 1), date(2025, 3, 2)),
            DayRange(date(2025, 3, 4), date(2025, 3, 4)),
        ),
    )


def test_many_updates_in_one_partition_collapse_to_one_day():
    rows = [
        {"last_upd_ts": SINCE + timedelta(minutes=i), "order_sold_ts": SINCE} for i in range(100)
    ]
    assert resolve_fake_rows(rows).partitions == (date(2026, 1, 1),)


def test_fake_rows_contract_includes_since_and_excludes_until():
    rows = [
        {
            "last_upd_ts": SINCE - timedelta(microseconds=1),
            "order_sold_ts": datetime(2025, 1, 1, tzinfo=UTC),
        },
        {"last_upd_ts": SINCE, "order_sold_ts": datetime(2025, 1, 2, tzinfo=UTC)},
        {
            "last_upd_ts": UNTIL - timedelta(microseconds=1),
            "order_sold_ts": datetime(2025, 1, 3, tzinfo=UTC),
        },
        {"last_upd_ts": UNTIL, "order_sold_ts": datetime(2025, 1, 4, tzinfo=UTC)},
    ]
    assert resolve_fake_rows(rows).partitions == (date(2025, 1, 2), date(2025, 1, 3))


def test_empty_results_and_empty_windows():
    assert resolve_fake_rows([]) == PartitionSelection((), ())
    assert resolve_partitions(CONFIG, ChangeWindow(SINCE, SINCE)) == PartitionSelection((), ())
    rows = [{"last_upd_ts": SINCE, "order_sold_ts": SINCE}]
    assert resolve_fake_rows(rows, ChangeWindow(SINCE, SINCE)) == PartitionSelection((), ())
    assert not discovery_required(CONFIG, ChangeWindow(SINCE, SINCE))


def test_null_partition_fails_explicitly_instead_of_dropping_data():
    rows = [
        {"last_upd_ts": SINCE, "order_sold_ts": SINCE},
        {"last_upd_ts": SINCE, "order_sold_ts": None},
    ]
    with pytest.raises(UnsupportedPartitionError, match="NULL"):
        resolve_fake_rows(rows)
    assert "IS NOT NULL" not in render_discovery_sql(CONFIG, WINDOW)


def test_different_columns_cannot_guess_partitions_without_discovery():
    assert discovery_required(CONFIG, WINDOW)
    with pytest.raises(ValueError, match="require discovered_partitions"):
        resolve_partitions(CONFIG, WINDOW)


def test_same_column_short_circuits_without_consuming_discovery():
    config = replace(CONFIG, partition_column="last_upd_ts")

    def unexpected_results():
        raise AssertionError("Same-column resolution must not consume discovery results")
        yield

    assert not discovery_required(config, WINDOW)
    assert resolve_partitions(config, WINDOW, unexpected_results()) == PartitionSelection(
        (date(2026, 1, 1),), (DayRange(date(2026, 1, 1), date(2026, 1, 1)),)
    )
    assert resolve_partitions(config, ChangeWindow(SINCE, SINCE)).partitions == ()


@pytest.mark.parametrize(
    "since, until, expected",
    [
        (datetime(2026, 1, 1, 12, tzinfo=UTC), UNTIL, (date(2026, 1, 1),)),
        (SINCE, UNTIL + timedelta(microseconds=1), (date(2026, 1, 1), date(2026, 1, 2))),
        (SINCE, SINCE + timedelta(microseconds=1), (date(2026, 1, 1),)),
        (
            datetime(2024, 2, 28, tzinfo=UTC),
            datetime(2024, 3, 1, tzinfo=UTC),
            (date(2024, 2, 28), date(2024, 2, 29)),
        ),
    ],
)
def test_same_column_window_boundary_days(since, until, expected):
    config = replace(CONFIG, partition_column="last_upd_ts")
    assert resolve_partitions(config, ChangeWindow(since, until)).partitions == expected


def test_window_offsets_normalize_to_utc_in_resolver_and_sql():
    offset = timezone(timedelta(hours=-5))
    window = ChangeWindow(
        datetime(2025, 12, 31, 19, tzinfo=offset),
        datetime(2026, 1, 1, 19, tzinfo=offset),
    )
    assert window == WINDOW
    assert render_discovery_sql(CONFIG, window) == render_discovery_sql(CONFIG, WINDOW)
    equivalent_midnight = datetime(2025, 12, 31, 19, tzinfo=offset)
    assert resolve_partitions(CONFIG, WINDOW, [equivalent_midnight]).partitions == (
        date(2026, 1, 1),
    )


def test_ranges_do_not_bridge_gaps_and_support_date_limits():
    result = resolve_partitions(CONFIG, WINDOW, [date.max, date.min, date.min, date(1, 1, 2)])
    assert result.ranges == (
        DayRange(date.min, date(1, 1, 2)),
        DayRange(date.max, date.max),
    )


@pytest.mark.parametrize("value", [None, "2026-01-01", 42, SINCE + timedelta(hours=1)])
def test_unsupported_discovery_values(value):
    with pytest.raises(UnsupportedPartitionError):
        resolve_partitions(CONFIG, WINDOW, [value])


def test_naive_discovery_timestamp_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_partitions(CONFIG, WINDOW, [datetime(2026, 1, 1)])


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"table": "table_orders"}, "explicit"),
        ({"table": "project_x.dataset_a.table_orders; DROP TABLE x"}, "identifier"),
        ({"table": "project_x.dataset_a.*"}, "identifier"),
        ({"change_column": "last_upd_ts + 1"}, "identifier"),
        ({"partition_column": ""}, "identifier"),
        ({"partition_column": "missing_ts"}, "Missing declared column"),
        ({"columns": ()}, "Missing declared column"),
        ({"granularity": "hour"}, "Only day"),
        ({"granularity": "month"}, "Only day"),
        ({"columns": [ColumnSpec("last_upd_ts", "TIMESTAMP")]}, "immutable tuple"),
        ({"columns": ("last_upd_ts",)}, "ColumnSpec"),
        (
            {
                "columns": (
                    ColumnSpec("last_upd_ts", "TIMESTAMP"),
                    ColumnSpec("LAST_UPD_TS", "TIMESTAMP"),
                )
            },
            "duplicate",
        ),
    ],
)
def test_unsupported_config_fails_before_rendering(changes, message):
    with pytest.raises(ValueError, match=message):
        replace(CONFIG, **changes)


@pytest.mark.parametrize("column", ["last_upd_ts", "order_sold_ts"])
@pytest.mark.parametrize("data_type", ["DATE", "DATETIME", "STRING", "timestamp"])
def test_both_columns_require_explicit_timestamp_types(column, data_type):
    columns = tuple(
        replace(spec, data_type=data_type) if spec.name == column else spec
        for spec in CONFIG.columns
    )
    with pytest.raises(ValueError, match="TIMESTAMP"):
        replace(CONFIG, columns=columns)


@pytest.mark.parametrize(
    "since, until, message",
    [
        (SINCE, SINCE - timedelta(microseconds=1), "until"),
        (datetime(2026, 1, 1), UNTIL, "since"),
        (SINCE, datetime(2026, 1, 2), "until"),
    ],
)
def test_invalid_window(since, until, message):
    with pytest.raises(ValueError, match=message):
        ChangeWindow(since, until)


@pytest.mark.parametrize(
    "zone", ["America/New_York", "Asia/Tokyo", "Etc/UTC", "utc", "", None, True, 42]
)
def test_partition_day_boundary_requires_exact_utc_declaration(zone):
    with pytest.raises(ValueError, match=r"partition_time_zone.*UTC.*#37"):
        replace(CONFIG, partition_time_zone=zone)


def test_partition_day_boundary_cannot_be_omitted():
    with pytest.raises(TypeError, match="partition_time_zone"):
        PartitionConfig(
            table=CONFIG.table,
            change_column=CONFIG.change_column,
            partition_column=CONFIG.partition_column,
            columns=CONFIG.columns,
        )


def test_config_and_window_are_frozen():
    with pytest.raises(FrozenInstanceError):
        CONFIG.table = "project_y.dataset_a.table_orders"
    with pytest.raises(FrozenInstanceError):
        CONFIG.columns[0].data_type = "DATE"
    with pytest.raises(FrozenInstanceError):
        WINDOW.since = UNTIL

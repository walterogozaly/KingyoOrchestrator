# Offline change-window to partition resolution

A metadata signal identifies a changed table and a reliable change timestamp
window. If `last_upd_ts` differs from the partition column `order_sold_ts`,
an update today can affect an order sold months ago. The change window alone
cannot identify the affected days.

Kingyo separates pure decisions in `core/partitions.py` from BigQuery-specific
SQL rendering in `adapters/bigquery_discovery.py`. Neither module opens a client,
loads credentials, reads metadata, executes SQL, or rewrites downstream SQL.
The CLI and poll loop are not wired to this interface.

## Supported input

- `PartitionConfig` is frozen and requires an explicit `project.dataset.table`,
  change column, partition column, and an immutable tuple of `ColumnSpec`
  declarations. Both named columns must exist in these caller declarations
  with exactly the type `TIMESTAMP`. Declarations are trusted input, not
  verification of an actual table schema.
- Only `granularity="day"` and UTC day boundaries are supported. Column,
  dataset, and table names must match `[A-Za-z_][A-Za-z0-9_]*`; the project
  component also permits hyphens after the first character. Expressions,
  wildcard tables, nested paths, backticks inside names, and arbitrary SQL
  are rejected. References must match declared column casing exactly;
  case-insensitive duplicate declarations are rejected.
- `ChangeWindow(since, until)` requires timezone-aware datetimes, normalizes
  them to UTC, and describes `[since, until)`. Reversed windows are rejected;
  equal endpoints represent an empty window.
- Missing columns, other types (including `DATE` and `DATETIME`), and other
  granularities raise `ValueError`. The caller must establish a reliable
  change column; this slice does not infer one or detect deletes that leave
  no changed row.

## Resolve without executing SQL

```python
from datetime import UTC, date, datetime

from kingyo_orchestrator.adapters.bigquery_discovery import render_discovery_sql
from kingyo_orchestrator.core.partitions import (
    ChangeWindow,
    ColumnSpec,
    DayRange,
    PartitionConfig,
    discovery_required,
    resolve_partitions,
)

config = PartitionConfig(
    table="project_x.dataset_a.table_orders",
    change_column="last_upd_ts",
    partition_column="order_sold_ts",
    columns=(
        ColumnSpec("last_upd_ts", "TIMESTAMP"),
        ColumnSpec("order_sold_ts", "TIMESTAMP"),
    ),
)
window = ChangeWindow(
    datetime(2026, 1, 1, tzinfo=UTC),
    datetime(2026, 1, 2, tzinfo=UTC),
)
assert discovery_required(config, window)
sql = render_discovery_sql(config, window)  # A string only; do not submit it.

# Fake discovery results supplied by the caller: late updates to old orders.
rows = [
    {"partition_start": datetime(2025, 3, 2, tzinfo=UTC)},
    {"partition_start": datetime(2025, 3, 1, tzinfo=UTC)},
    {"partition_start": datetime(2025, 3, 2, tzinfo=UTC)},
]
selection = resolve_partitions(config, window, (row["partition_start"] for row in rows))
assert selection.partitions == (date(2025, 3, 1), date(2025, 3, 2))
assert selection.ranges == (DayRange(date(2025, 3, 1), date(2025, 3, 2)),)
```

The renderer emits `SELECT DISTINCT TIMESTAMP_TRUNC(partition_column, DAY, 'UTC')`
as `partition_start`, with `change_column >= since AND change_column < until`.
This uses BigQuery's [timestamp truncation](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc).
The SQL includes microsecond-precision UTC timestamp literals and quotes validated
identifiers. It has no partition-date restriction: adding a recent-date filter
would lose late updates.

For different columns, `resolve_partitions` requires caller-supplied discovered
values, even when the result is an empty iterable. It accepts Python dates or
timezone-aware timestamps representing UTC midnight. Strings, naive timestamps,
and non-midnight timestamps are rejected. A returned NULL raises
`UnsupportedPartitionError`, preserving an explicit signal that the caller must
handle the NULL partition; NULLs are never silently discarded by SQL or the resolver.

The result is frozen: `partitions` is a sorted, unique tuple of UTC dates;
`ranges` merges contiguous dates only. Each `DayRange(start, end)` has
**inclusive** endpoints, suitable for a future date filter `start <= day <= end`.
Gaps remain gaps. These are logical day values, not physical partition IDs.

When both column names are identical, `discovery_required` is false and
`resolve_partitions(config, window)` derives all UTC days intersecting the
window without consuming discovery values. An exclusive `until` at midnight
does not include that day. This is a conservative window-derived set: it can
include days with no rows, because populated days are unknowable without a read.
Empty windows always return empty selections and require no discovery.
The renderer itself remains callable and always returns a SQL string; callers
use `discovery_required` to skip unnecessary discovery.

## Known limits found by audit #25

Two defects are open against this module; both are tracked with witnesses in
`tests/audit/test_partition_audit.py`.

- The resolved days are **UTC** days and `PartitionConfig` cannot express the
  table's partition day boundary. For a table partitioned by
  `DATE(order_sold_ts, 'America/New_York')` (or any non-UTC zone) the selection
  silently misses the partition that holds a changed row. Only `granularity` is
  enforced; the UTC assumption is trusted, not checked. See #37.
- A contiguous window is materialized one `date` per day before being collapsed
  back into a single `DayRange`, so a wide window (a first run with no stored
  watermark) costs seconds and hundreds of megabytes. See #38.

## Cost and caller responsibilities

Discovery references only the change and partition columns. A qualifying constant
filter on the actual partition column can prune partitions, but the emitted SQL
filters the change column only. When the columns differ, partitioning on the sold
timestamp alone does not bound this scan. A table that requires a partition filter
may reject the generated SQL. See BigQuery's [partition pruning and required filters](https://docs.cloud.google.com/bigquery/docs/querying-partitioned-tables).

Clustering on the change column can reduce scanned blocks with this simple range
predicate; it does not guarantee a fixed byte limit. Without useful pruning,
discovery can scan those two columns across the entire table even for a tiny
window. See BigQuery's [clustered-table block pruning](https://docs.cloud.google.com/bigquery/docs/querying-clustered-tables).

Do not infer an affordable query from the window size or add a sold-date cutoff
unless an externally established lateness bound guarantees completeness. Before
any later query execution, the caller must obtain explicit authorization, assess
cost outside this offline slice (for example via an authorized dry run), and apply
an appropriate budget. If the estimate or required partition filter makes complete
discovery infeasible, stop and agree on a bounded discovery source or supported
lateness policy. This implementation neither performs a dry run nor executes a
paid query, and it never changes table layout, IAM, or cloud resources.

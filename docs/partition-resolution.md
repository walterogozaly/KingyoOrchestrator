# Offline change-window to partition resolution

A metadata signal identifies a changed table and a reliable change timestamp
window. If `last_upd_ts` differs from the partition column `order_sold_ts`,
an update today can affect an order sold months ago. The change window alone
cannot identify the affected days.

Kingyo separates pure decisions in `core/partitions.py` from BigQuery-specific
SQL rendering in `adapters/bigquery_discovery.py` and
`adapters/bigquery_partition_filter.py`. None of these three modules opens a
client, loads credentials, reads metadata, executes SQL, or rewrites full
downstream SQL. The filter renderer returns predicate strings
for a caller to insert explicitly.
The CLI and poll loop are not wired to this interface.

## Supported input

- `PartitionConfig` is frozen and requires an explicit `project.dataset.table`,
  change column, partition column, and an immutable tuple of `ColumnSpec`
  declarations. Both named columns must exist in these caller declarations
  with change type exactly `TIMESTAMP` and partition type `DATE` or
  `TIMESTAMP`. Declarations are trusted input, not
  verification of an actual table schema.
- Only `granularity="day"` and `partition_time_zone="UTC"` are supported.
  The timezone declaration is **required**, with no default or inference;
  existing callers must add it. Omitting it raises `TypeError`, and any value
  other than the exact string `"UTC"` raises `ValueError` naming issue #37,
  including equivalent aliases such as `"Etc/UTC"`. Both granularity and the
  declaration are validated before discovery rendering or window resolution.
  The actual table's partition expression is **trusted**, not read or verified:
  callers must establish that its day boundary is UTC before declaring it.
  Misdeclaring a non-UTC table as UTC can still yield incorrect days.
- Column, dataset, and table names must match `[A-Za-z_][A-Za-z0-9_]*`; the project
  component also permits hyphens after the first character. Expressions,
  wildcard tables, nested paths, backticks inside names, and arbitrary SQL
  are rejected. References must match declared column casing exactly;
  case-insensitive duplicate declarations are rejected.
- `ChangeWindow(since, until)` requires timezone-aware datetimes, normalizes
  them to UTC, and describes `[since, until)`. Reversed windows are rejected;
  equal endpoints represent an empty window.
- Missing columns, other types (including `DATETIME`), and other
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
    partition_time_zone="UTC",
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

For a `TIMESTAMP` partition column, discovery emits
`SELECT DISTINCT TIMESTAMP_TRUNC(partition_column, DAY, 'UTC')` as
`partition_start`. For a `DATE` partition column it selects the raw column
`DISTINCT`, with no truncation or cast. Both use
`change_column >= since AND change_column < until`.
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

## Literal downstream partition filters

The two steps stay separate: render discovery SQL, then pass caller-supplied
results through `resolve_partitions` and render the selected days as constants.
`render_partition_filter(config, selection, alias=None)` in
`adapters/bigquery_partition_filter.py` emits:

- For DATE columns, an `IN (DATE '...', ...)` list on the raw column.
- For TIMESTAMP columns, `column >= TIMESTAMP '...' AND column < TIMESTAMP '...'`
  for each contiguous UTC day range. Disjoint ranges are joined with a
  parenthesized OR, preserving gaps and safe composition with other predicates.
- For an empty selection, `FALSE`, never an empty `IN ()` or an unrestricted filter.

No subquery, join, CAST, or function wraps the filtered column. An optional alias
must be a simple identifier; alias and column are quoted separately.
The renderer follows this literal-only policy instead of generating expression
filters whose pruning would need separate verification.

The two single-range output shapes (`DATE '...'` in-list and one half-open
`TIMESTAMP` range) are qualifying constant filters on the partition column and
can enable [static partition pruning](https://docs.cloud.google.com/bigquery/docs/querying-partitioned-tables).
The third shape, the parenthesized OR of disjoint ranges, is correct
gap-preserving SQL but its pruning benefit is unverified: it is the typical output
here, because gaps between late partitions are the point of the resolver, and
BigQuery does not document OR-of-ranges as prunable.

Continuing the offline example above with a DATE partition column:

```python
from dataclasses import replace

from kingyo_orchestrator.adapters.bigquery_partition_filter import render_partition_filter

config_date = replace(
    config,
    partition_column="partition_date",
    columns=(ColumnSpec("last_upd_ts", "TIMESTAMP"), ColumnSpec("partition_date", "DATE")),
)
selection_date = resolve_partitions(config_date, window, [date(2026, 10, 2), date(2026, 10, 1)])
filter_sql = render_partition_filter(config_date, selection_date, alias="T")
assert filter_sql == "`T`.`partition_date` IN (\n  DATE '2026-10-01',\n  DATE '2026-10-02'\n)"
```

The renderer sorts/de-duplicates the actual `selection.partitions` and derives
fresh ranges, ignoring any inconsistent caller-supplied range metadata.
`select_partitions(days)` is the pure helper for building canonical selections
from Python dates. Discovery continues to preserve NULL for explicit rejection.
The same-column shortcut still applies only to TIMESTAMP declarations, since the
change column must remain TIMESTAMP.

More than **1,000 unique days** raises `ValueError` for either partition type;
there is no silent truncation or broadening. Exactly 1,000 is supported. The caller
must handle larger selections explicitly (for example, by batching a later
approved operation). A TIMESTAMP selection containing `9999-12-31` also raises
`ValueError`: its next-day exclusive upper bound is unrepresentable. DATE literals
support that date directly.

A planned later check is an authorized BigQuery dry run of the same synthetic
query with and without the literal partition filter, comparing bytes processed.
This issue performs neither dry runs nor query execution, and does not rewrite
or run downstream models.

## Known limits found by audit #25

Issue #37 is addressed by requiring an explicit UTC declaration and rejecting
other declarations on both paths; `tests/audit/test_partition_audit.py` retains
the synthetic boundary witnesses as passing refusal tests and UTC controls.
This does not verify the caller's statement about the physical table.

The wide-window performance finding remains open:

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

# Narrow delta staging SELECT

`adapters.bigquery_delta.render_delta_select` renders a SELECT string for changed
rows. It never creates a staging table, reads metadata, runs a query or dry run,
or contacts BigQuery. `core.delta.DeltaConfig` validates the caller's projection
inputs independently of the renderer. The CLI does not use this API yet.

## Supported inputs

`DeltaConfig` is frozen and takes:

- `table`: explicit `project.dataset.table`, with the same component rules as
  `PartitionConfig`: letters/underscore initially, letters/digits/underscore thereafter;
  a project component may also contain hyphens after its first character.
- `key_columns`: a nonempty immutable tuple; caller order is preserved.
- `partition_column`: one column name, whether or not it is also a key.
- `declared_columns`: an immutable tuple naming the full supported schema.
- `changed_columns`: an immutable tuple representing the changed-column set; defaults to `()`.

Column names use `[A-Za-z_][A-Za-z0-9_]*`; nested fields, expressions, wildcards,
quoted inputs, and whitespace are unsupported. All references must match declared
names exactly, including case. Each tuple rejects case-insensitive duplicates.
Convert external collections to tuples before constructing the config. Overlap
across keys, partition, and changes is allowed and deduplicated in the projection.

Projection order is exactly keys in caller order, partition if not already a key,
then other changed columns alphabetically. Every projected name appears once and
is backtick-quoted. No extra declared column or wildcard is added.

An empty changed tuple means **rows touched, no column values changed** and produces
only keys plus partition. Keys and partition are always included even if unchanged.
The renderer does not derive or verify the external changed-column signal.

The optional `predicate` is an expression string appended verbatim after `WHERE `.
Use `None` to omit WHERE; blank or non-string predicates raise `ValueError`.
Do not include the WHERE keyword itself. Whitespace and newlines are preserved;
predicate syntax, column references, and row selectivity are the caller's responsibility.
No SQL parsing, quoting, or rewriting is performed on this expression.

## Offline example

```python
from kingyo_orchestrator.adapters.bigquery_delta import render_delta_select
from kingyo_orchestrator.core.delta import DeltaConfig

config = DeltaConfig(
    table="project_x.dataset_a.table_orders",
    key_columns=("order_id",),
    partition_column="partition_date",
    declared_columns=("order_id", "partition_date", "amount", "status", "notes"),
    changed_columns=("status", "amount"),
)
sql = render_delta_select(config, "`partition_date` IN (DATE '2026-01-01')")
print(sql)
```

```sql
SELECT
  `order_id`,
  `partition_date`,
  `amount`,
  `status`
FROM `project_x.dataset_a.table_orders`
WHERE `partition_date` IN (DATE '2026-01-01')
```

The caller can also supply a predicate from the partition filter renderer. This
renderer composes with that work through text and does not depend on its implementation.

## Bytes and future verification

BigQuery recommends selecting only needed columns: excess projection adds read
I/O and result materialization. A 50-column schema with one key, a separate
partition column, and four other changed columns yields exactly six projected
columns here. See Google's [projection guidance](https://docs.cloud.google.com/bigquery/docs/best-practices-performance-compute#avoid_select_).
Predicate columns can still require reads even when omitted from SELECT; a narrow
projection does not by itself bound scanned rows or guarantee a fixed savings ratio.

A later, separately authorized check should compare BigQuery dry-run bytes for
wide versus narrow SELECTs over the same explicitly supplied table and predicate.
Record estimated bytes processed and confirm the required projected columns before
considering execution. This issue does not run either query or dry run.

Column-lineage derivation, downstream model rewriting, staging-table writes,
execution, costs, and CLI wiring remain outside this renderer.

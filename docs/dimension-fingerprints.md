# Small-dimension content fingerprints

Some dimensions update a load timestamp on every refresh while their attributes
stay the same. A normal metadata signal then suggests work that the content does
not require. This offline API proposes a content check for explicitly configured
small tables. It renders SQL and consumes supplied results; it never runs SQL,
dry runs, metadata reads, credential discovery, or persistence.

## Configured projection and SQL

`FingerprintConfig` is frozen. Supply an explicit `project.dataset.table`, nonempty
`key_columns`, `content_columns`, `audit_columns`, and positive `size_threshold_bytes`.
All column collections are immutable tuples. Names follow the partition config's
simple identifier rules. Each collection rejects case-insensitive duplicates and
the three collections must be disjoint. There are no audit-name defaults; any name
and any number of audit columns come from the table's config. `row_hash` is the
per-key output alias and cannot be a key name.

`columns_hashed` is keys in caller order followed by content columns alphabetically.
Both SQL renderers hash exactly these columns with
`FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))`. Audit columns never appear in the
generated SQL. The caller must guarantee one row per unique, non-NULL key; the
per-key SELECT emits one row per key without guessing how to combine duplicate rows.

- `render_fingerprint_sql`: one row with `content_hash` (COALESCE of `BIT_XOR(row_hash)`
  to zero) and `row_count`. It uses `COUNT(1)`, equivalent to counting all rows with
  `COUNT(*)`, while keeping wildcard characters out of the rendered SQL.
- `render_key_hashes_sql`: the configured key columns plus `row_hash`; use these
  results to construct a key/hash mapping when the aggregate differs.

An optional `partition_filter` is preserved verbatim but must match the supported
style: one key/content column, optionally backtick-quoted, followed by `IN` with
one or more literal `DATE 'YYYY-MM-DD'` values. Dates are validated. Audit or unknown
columns, expressions, subqueries, blank predicates, and other shapes are rejected.
Use `None` for no filter. This narrow contract permits a compatible literal partition
filter from another renderer without evaluating it.

## Decisions, storage records, and key diffs

Call `decide(config, table_size_bytes, previous, current)` using the caller's
nonnegative metadata size. Size at or below the configured threshold is eligible.
Above it, `not_eligible` says to fall back to the normal signal and requires no query
result (`current` may be `None`). Check this gate before arranging any future query.
An eligible table requires a current `Fingerprint` with exactly `config.columns_hashed`.

- `no_baseline`: no previous record, or the hashed column list changed; store the
  new baseline and treat the observation as changed.
- `unchanged_suppress`: hash and row count match; suppress the normal signal.
- `changed`: hash or row count differs; per-key results can identify changed keys.

Every decision includes a reason. `computed_at` records observation time and does
not determine content equality. `Fingerprint.to_dict()` and `from_dict()` round-trip
JSON-safe fields: `hash` (signed 64-bit integer), `row_count`, `columns_hashed` (array),
and `computed_at` (timezone-aware ISO 8601 string, normalized to UTC). No file store
is added. Bind baselines to the same table, key scheme, types, and partition scope;
reset them when those change. A different filter is a different observation scope.

`diff_keys(previous, current)` consumes unique-key/hash mappings and returns frozen
`added`, `removed`, and `modified` key sets. Composite tuple keys are supported.
Missing per-key history requires a new baseline, not a guessed diff.

## Audit detection and confirmation

`propose_audit_columns(previous_rows, current_rows, column_types)` consumes snapshots
as mappings from row keys to complete row dictionaries with exactly the declared
columns. Supply consistent row value encodings and correct declared types. Detection
requires the same key set and at least two matched rows; empty, one-row, added/removed,
or NULL timestamp cases produce no candidate. Only declared TIMESTAMP/DATETIME
columns can be candidates, regardless of name:

- `high`: that column changed on every matched row and all other columns stayed equal.
- `low`: it retained a single non-NULL value across every row in both snapshots.

A timestamp changing on only some rows is not proposed; one changing together with
attributes is not proposed either. Low-confidence uniformity does not establish
audit semantics. Both heuristics can misclassify a real attribute, and excluding
that attribute would hide genuine changes.

Default to report-only: inspect candidates, then confirm audit names in config and
remove them from `content_columns`. `apply_audit_policy` respects an existing audit
config and otherwise returns it unchanged by default. With explicit `auto_apply=True`,
the last two `SnapshotPair` values must be consecutive (the first current equals the
second previous). A content column is moved into audit only if it is high confidence
in both pairs. Low, partial, broken, or single-pair evidence never auto-excludes it.
This opt-in rule remains a heuristic; manual confirmation is preferable for ambiguous
business timestamps. A new exclusion changes the hashed-column schema and needs
a new comparable fingerprint baseline.

## Runnable offline example

```python
from kingyo_orchestrator.adapters.bigquery_fingerprints import render_fingerprint_sql
from kingyo_orchestrator.fingerprints import Fingerprint, FingerprintConfig, decide

config = FingerprintConfig(
    table="project_x.dataset_a.dim_customer",
    key_columns=("customer_id",),
    content_columns=("name",),
    audit_columns=("run_clock",),  # configured for this synthetic table only
    size_threshold_bytes=10 * 1024 * 1024,
)
assert decide(config, 20 * 1024 * 1024, None, None).status == "not_eligible"
print(render_fingerprint_sql(config))  # text only; run_clock is absent
current = Fingerprint(123, 2, config.columns_hashed, "2026-01-01T12:00:00Z")
assert decide(config, 1024, None, current).status == "no_baseline"
assert decide(config, 1024, current, current).status == "unchanged_suppress"
```

## Costs and correctness limits

An aggregate's one-row result still requires reading the projected content. Google's
[on-demand pricing](https://cloud.google.com/bigquery/pricing#on_demand_pricing) documents
a 10 MB minimum per referenced table and per query. A caller choosing a 10 MiB policy
threshold supplies 10,485,760 bytes; that threshold is a separate eligibility policy,
not a promise about billing. For tiny dimensions the billing floor can dominate;
large content scans may cost more than the rebuild they avoid. No default threshold
or savings estimate is inferred here. A later authorized dry run should compare
estimated bytes for these strings and the proposed rebuild, with an explicit project.

XOR plus count is order-independent and checks more than a hash alone, but suppression
is probabilistic: 64-bit hash collisions and XOR cancellation can hide content changes
even at equal row counts. Per-key hashes also carry collision risk. This is not an
exact change detector. See [FarmHash behavior](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint).

`TO_JSON_STRING` represents SQL NULL fields as JSON null, and floating-point special
values have JSON encodings. No tolerance, numeric rounding, or business-value
normalization is added. Keep types, column order, and encodings stable; floating-point
noise can signal a change. See [JSON encodings](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_encodings).

Large-table checks, executions, dry runs, persistence, and poll-loop integration remain
outside this slice. Tests use fake hashes and synthetic rows, not a BigQuery emulator.

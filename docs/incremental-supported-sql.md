# Kingyo supported style (v0 contract)

Kingyo runs plain Dataform SQLX incrementally **when the SQL follows the patterns below**. Anything else still runs,
as a full refresh of that model, so correctness never depends on following the style; only the savings do.
Run `python -m kingyo_orchestrator.incremental.cli --repo <repo> --db <warehouse.duckdb> check` to see, per dependency, which pattern applies
and what to change when it does not. Every pattern here is covered by a test that compares the incremental result
with a full rebuild (named in brackets).

## 1. Sources (declarations)

| Rule | Why |
|---|---|
| Declare `bigquery: { partitionBy: "DATE(<ts>)" }` on every source that sends change signals. | Signals name partitions of this expression. |
| If rows are **updated in place** (upsert), declare `uniqueKey: "<col>"` (one column). Without it Kingyo assumes the source is **append-only**. | An updated row leaves its old version in an older partition downstream; the key is how Kingyo finds it. [`test_upsert_with_late_update`, `test_random_upserts_match_full_rebuild`] |
| **Deletes in sources are not supported yet.** A deleted row is invisible to a partition signal. Signal `ALL` for that table after deletes. | Known gap. |
| Signal format: `kingyo signal <table> <partition values...> --observed <time>`; `ALL` when unsure. | |
| Optional: add `--columns c1 c2` when you know that **only** these columns changed in existing rows (no inserts, no deletes, partition column unchanged). Children that don't read them are skipped; children that only *select* them get just those output columns marked. Omit it whenever new rows arrived. | Column-aware relevance. [`test_unread_column_change_is_a_no_op`, `test_random_column_updates_match_full_rebuild`] |

## 2. Models: the four incremental patterns

Each model declares `partitionBy` over **its own output columns**. Unpartitioned models are always fully refreshed (cheap if small).

**A. Partition-aligned (best).** The model's partition column is passed through, row by row, from the changed parent's partition column
(directly, or through `DATE()`), possibly via CTEs or subqueries. Aggregations and windows must include it in `GROUP BY` / `PARTITION BY`.
Cost: only the changed days are read and rewritten. [`stg_orders`, `daily_revenue`, `orders_enriched`; `tests/test_flatten.py`]
```sql
select date(last_upd_ts) as day, sum(amount) as revenue from ${ref("stg_orders")} group by 1   -- partitionBy: "day"
```

**B. Partitioned by another column of the parent** (e.g. `order_date` when the parent is partitioned by `last_upd_ts`).
Kingyo looks up which `order_date` values the changed rows touch (old and new). [`orders_by_order_date`]

**C. Keyed: "latest per key", per-key aggregates.** Every `GROUP BY` / `PARTITION BY` / `DISTINCT` that reads the parent is keyed
by **one column K of the parent**, and **K is in the model's output**. Kingyo recomputes only the keys present in the changed partitions
and moves their rows between partitions. [`latest_orders` (CTE + GROUP BY), `latest_status` (ROW_NUMBER subquery); `tests/test_analyzer_regressions.py`]
```sql
select customer_id, max(last_upd_ts) as last_upd_ts, sum(amount) as amount from ${ref("stg_orders")} group by customer_id
```
Write "latest per key" as `row_number() over (partition by K ...)` in a subquery filtered `rn = 1`; `QUALIFY` is also fine when
partitioned by K. Keep keyed chains short: each keyed hop can widen the set of partitions that change.

**D. Changed table on the nullable side of a LEFT JOIN** (a dimension). Join with an equality on one column
(`left join ${ref("dims")} d on f.dim_id = d.id`), in a plain row-by-row SELECT. Kingyo recomputes the rows whose join key changed.
Best when the preserved side's join column is in the output. [`tests/test_outer_join_wired.py`, `tests/test_outer_join.py`]

## 3. Shapes that always fall back to a full refresh

- `LIMIT` / `OFFSET`, global aggregates (`select count(*) from t` with no `GROUP BY`), `UNION DISTINCT`
- `GROUP BY` / windows not keyed by the partition column or a single output key
- the changed table read twice (self-join, a CTE over it referenced twice, `WHERE x > (select avg(..) from parent)`)
- the changed table on the nullable side of a join inside an aggregate, or joined on more than one column
- `${...}` JavaScript values inside SQL other than `ref()`/`self()` (until Kingyo reads `dataform compile --json` output)
- non-deterministic SQL (`current_timestamp()`, `rand()`, `LIMIT` with ties): results can differ from any rebuild

## 4. Runtime behaviour a test user can rely on

- A run is `kingyo run` on any schedule; state lives in the warehouse, a killed run resumes. [`test_crash_mid_run_is_resumable`]
- Signals younger than the settle window (15 min) wait for the next run. [`test_fresh_signal_waits_then_acts_on_later_wake`]
- If a change touches more than 25% of a table's partitions (or keys), Kingyo rebuilds that table instead (tables over 10k rows).
- A recomputed partition whose content did not change stops propagation, on tables under 10k rows (above that the hash
  costs more than it saves, so Kingyo assumes recomputed partitions changed). [`test_early_cutoff_when_nothing_changes`]
- A change to columns a child never reads, or reads only in its SELECT list, does not make the child recompute (or makes
  its own children see only the derived columns as changed). A changed column used in WHERE / JOIN ON / GROUP BY /
  window / ORDER BY / DISTINCT, or behind `SELECT *` Kingyo can't expand, counts as "everything changed".

## Known costs (prototype DuckDB benchmark, 7.3M synthetic rows)
One new day: 0.53-0.62x a full rebuild (0.34x in one later run). New day + updates to recent days: 0.90-0.99x. Updates scattered across many old days can cost
more than a rebuild (up to 3-4x); the cost rules are being moved to byte estimates (see KPI.md).

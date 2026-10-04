# Kingyo benchmark harness (offline DuckDB first, BigQuery later)

    pip install -e ".[bench]"          # duckdb + pinned benchbox (data generator)
    python bench/harness.py --sf 0.05 --seeds 5 --repeats 3 --out bench/results/run1
    python bench/harness.py --candidate kingyo-prototype --repeats 0 --jobs 4   # fast loop: exact rows scanned + correctness, trials in parallel, no wall time (about 5x faster than --repeats 3)
    python bench/harness.py --candidate kingyo-prototype          # the in-repo incremental prototype
    python bench/harness.py --candidate kingyo-prototype-columns  # + changed-column hints (late updates, dimension change, added column)
    python -m pytest tests/test_bench.py -q   # offline, tiny hand-written data; skipped without duckdb
    python -m pytest bench -q                  # correctness checker and report tests

`--jobs` needs `--repeats 0` on purpose: wall time taken while trials compete for cores is not a paired, randomized run, so the timed pass stays serial.
Rows scanned varies by well under 1% between identical runs (DuckDB scan scheduling), so compare ratios, not exact row counts.

The default candidate `full-rebuild` is a control: it must reproduce the baseline exactly with ratios near 1.

What it does: SSB data (pinned BenchBox generator) becomes sources of a Dataform-style pipeline (`ssb_repo/`: staging, star-join fact,
daily mart, the 13 SSB flights adapted to per-year marts; "SSB-derived", not the official queries). For each scenario and seed the same source
mutation is applied to two copies of a built warehouse: baseline = full rebuild, candidate = Kingyo signal + run_once. Output must match
the baseline exactly (multiset per model) before the trial counts as faster/same/slower; otherwise it is reported INCORRECT.
Work metric offline = rows scanned by table scans (DuckDB profiling), plus statements and wall time (median of repeats, random order).
Tables are clustered by partition expression with small row groups so zone maps act as a proxy for partition pruning
(`CAST(d AS VARCHAR) IN` does not prune, `d IN (DATE ...)` does, like BigQuery). It is a proxy: real bytes/slot-ms need BigQuery runs from a maintainer's machine.

Scenarios: unchanged_no_signal, unchanged_resignal, new_day, backfill_7_days, late_updates_recent, late_updates_scattered,
dimension_change, source_column_added, edge_mix (NULLs, unmatched key, moved dates, filtered-out rows), model_sql_change (unsupported yet, kept visible).
Separate tracks (not mixed here): SQL-only rewrites, layout/materialization changes. Delegable pieces: see SOL_ISSUES.md.
BenchBox verdict so far: usable as the SSB data generator (pinned 0.4.1, beta, emits a version-inconsistency warning); its runner/BigQuery driver is not used.

## Reports from saved trials

```shell
python -m bench.report bench/results/run1.json > report.md
python -m bench.report bench/results/run2.json --baseline-json bench/results/run1.json > comparison.md
python -m bench.report bench/results/run2.json --metrics rows_scanned seconds --faster-threshold 0.9 --slower-threshold 1.1
python -m pytest bench -q
```

Reporting needs only Python's standard library and reads local JSON; it does not run
the harness, open a database, or contact BigQuery. From Python use
`format_report(trials, metrics=("rows_scanned", "seconds"))` from `bench.report`;
optional keywords are `baseline_trials`, `faster_threshold`, and `slower_threshold`.
The previous run's trials are distinct from the baseline side of each paired trial.

Every scenario and non-correct trial remains visible. Only input statuses
`faster`, `same`, and `slower` contribute to ratios; `incorrect`, `failed`, and
`unsupported` never receive speed credit. Correct trials are classified against
the first requested metric with both baseline and candidate values, or retain
their supplied status if none is available. Default thresholds are faster below
0.95 and slower above 1.05; boundary values count as same.

Metric cells show the geometric mean of candidate / baseline ratios, minimum,
maximum, reciprocal speedup, and contributing trial count. Default metrics are
`rows_scanned`, `seconds`, `statements`, `total_slot_ms`, `bytes_processed`, and
`bytes_billed`. Metrics absent on either side are excluded from that metric's
average; columns without any paired values are omitted. Metrics present only on non-correct
trials remain visible with no average. No
legacy `scan_ratio` or `time_ratio` field is needed. Negative, non-finite, or
non-numeric comparable values are rejected.

Zero / zero counts as ratio 1. Positive work against a zero baseline is assigned
ratio 1,000,000, so it never receives speed credit. Other ratios below 1e-6 are
clamped to 1e-6 before logarithms; resulting speedups are capped at 1,000,000x.
The report therefore keeps zero-work cases finite and visible.

Previous-run comparisons use each scenario's correct-trial geometric mean for
each available metric. Negative percentage changes mean improvement. Regressions
include ratios increasing by more than `slower_threshold - 1` (default 5%),
increased incorrect/failed counts, and scenarios missing from the current run.
New scenarios and unavailable metric pairs show `-` instead of fabricated deltas.
These are descriptive comparisons, without statistical significance claims;
compare equivalent scenario/seed populations for meaningful conclusions.

The harness's existing inline summary remains unchanged; this command formats
its saved JSON independently. Report tests are included in the normal pytest run
and require no optional benchmark dependencies.

## Correctness checker (`compare.py`)

`compare_tables(con_a, con_b, table, *, ordered_by=None, float_tol=1e-9)` returns `[]` when the two sides
are equal, otherwise human-readable problems with at most 5 sample differing rows. `compare_dag(con_a,
con_b, tables, **kw)` does the same for several tables and returns one entry per table, empty when that
table matches. `table` is one name looked up on both connections, or a `(name_a, name_b)` pair when both
tables share one connection. Side a is the baseline and side b the candidate.

```python
from bench import compare

problems = compare.compare_dag(ca, cb, ["q1_1", "daily_revenue"])
problems = compare.compare_dag(ca, cb, {"daily_revenue": ["order_date"]})   # order matters
```

Rules: same column names and order, else a "columns differ" problem; multiset equality by default, so
duplicate counts matter; NULL equals NULL, NaN equals NaN, floats equal within relative `float_tol`
(pass `0` for exact); timestamps, dates and decimals compare exactly. With `ordered_by`, rows must match
in that order and rows tied on it are compared as multisets, so an ordering tie is never a difference.

Memory: when both tables share one connection and no tolerance is needed, one `EXCEPT ALL` query decides
equality and no row reaches Python. Otherwise DuckDB sorts both sides and the checker streams them in
chunks, merging one row per side at a time. Table names must be plain identifiers, and an unreadable
table is reported as a problem rather than raised.

Caveat: float tolerance is matched greedily in sorted order, so in a pathological non-transitive case it
can report a difference where a perfect matching exists. That direction is the safe one: a false problem
costs a re-run, a false equality would hide a wrong result.

Tests are in `bench/test_compare.py`, offline, synthetic rows only.

Not wired into the harness yet: `harness.compare` is unchanged and a follow-up PR from the harness owner
swaps it for `compare_dag`.

## Deterministic source variants

List the seven available variants without importing DuckDB or generating data:

```console
python -m bench.variants --list
python -m pytest bench/test_ssb_variants.py -q
```

`bench.ssb_variants.apply_variant(con, name, seed)` mutates only the five SSB source
tables in a caller-owned **local DuckDB** connection. Load a fresh source copy for
each variant/seed, apply the variant, then build the derived warehouse. This module
does not call BenchBox, import a cloud client, or modify the harness.

```python
from bench.ssb_variants import apply_variant

# con already contains sources from ssb_data.load_sources(con, data_directory).
apply_variant(con, "nulls", seed=42)
# The harness owner builds all derived models after applying the variant.
```

| Variant | Property on a fresh source load |
| --- | --- |
| `clean` | No changes and no queries. |
| `nulls` | NULLs in 20% (rounded up) of each fact measure/discount and selected customer/supplier/part attributes; dimension keys remain present. |
| `duplicates` | Copies 10% (rounded up) of each dimension and of facts. Fact copies keep identical business fields with new unique `lo_rowid` values above the previous maximum. |
| `unmatched_keys` | 20% (rounded up) of facts per join receive a customer/supplier/part key or business date outside that dimension's range. Selections may overlap. |
| `empty_partitions` | Removes all facts on 20% (rounded up) of observed business dates, leaving at least one business date; empties `supplier`. Load timestamps on surviving rows are unchanged. |
| `skewed_keys` | Exactly ceil(90% of facts) share three existing customers evenly; the remainder use a fourth customer. |
| `ties` | At least half the facts share an existing `lo_orderdate`/`lo_loaded_ts` pair, while retaining distinct `lo_rowid`. |

Selection uses a stable MD5 ordering of the source key, seed, and selection purpose;
it does not depend on insertion order or global random state. Identical fresh data,
variant, and integer seed reproduce identical tables. Different seeds are tested
against one another; finite tiny sources can still give coinciding selections.
Non-clean variants require at least ten facts with unique non-NULL `lo_rowid`,
nonempty dimensions with unique non-NULL keys, and the schema from `load_sources`.
Skew also requires four customers; empty partitions require two business dates.
Invalid shapes reject clearly. Apply one variant per fresh load; composition or
reapplication is not supported. Mutations use one transaction and roll back on
failure. Begin outside a caller transaction; DuckDB rejects nested transactions.

For a future harness `--variant` option, include variant/seed in the base-cache key,
apply it immediately after `load_sources` and before `pipeline.build_all`, and copy
that same built base for both candidate and baseline. Do not apply it separately
during scenario mutations. `harness.py` has no such option in this PR.
Upstream layouts: `--layout {load_ts,order_date,unpartitioned}` changes how the fact source is partitioned
(change column, a different business date, or none); see `upstream_layouts.py` and `results/upstream_layouts_sf0.05.md`.

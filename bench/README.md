# Kingyo benchmark harness (offline DuckDB first, BigQuery later)

    pip install -e ".[bench]"          # duckdb + pinned benchbox (data generator)
    python bench/harness.py --sf 0.05 --seeds 5 --repeats 3 --out bench/results/run1
    python bench/harness.py --candidate kingyo-prototype ...   # needs KINGYO_PROTOTYPE_PATH (prototype not in this repo yet)
    python -m pytest tests/test_bench.py -q   # offline, tiny hand-written data; skipped without duckdb

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

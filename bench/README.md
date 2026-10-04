# Kingyo benchmark harness (offline DuckDB first, BigQuery later)

    pip install -e ".[bench]"          # duckdb + pinned benchbox (data generator)
    python bench/harness.py --sf 0.05 --seeds 5 --repeats 3 --out bench/results/run1
    python bench/harness.py --candidate kingyo-prototype          # the in-repo incremental prototype
    python bench/harness.py --candidate kingyo-prototype-columns  # + changed-column hints (late updates, dimension change, added column)
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

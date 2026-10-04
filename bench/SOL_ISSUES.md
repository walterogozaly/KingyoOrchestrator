# Issue-ready specs for Sol (benchmark/eval track)

Common rules for every issue below (put them in each issue body):
- Public repo: no real project, dataset, account or host identifiers; synthetic names only (`example-project`, `bench_ds`).
- Offline by default: tests must not need cloud credentials or network beyond `pip install`. Pin `benchbox==0.4.1` where used.
- Code goes under `bench/` of KingyoOrchestrator (`bench/` is self-contained: standard library, `duckdb`, `sqlglot`, `pytest`; it does not import the orchestrator prototype).
- Run `python -m ruff check .`, `python -m ruff format --check .`, `python -m pytest -q`. Update `bench/README.md` for any new command.
- Do not touch `bench/harness.py`, `bench/scenarios.py`, `bench/ssb_repo/` (owned by the Kingyo-pipeline thread); if an interface needs a change, say so in the PR.
- No BigQuery runs. Anything that talks to BigQuery is built and unit-tested against fakes; real runs happen later on a maintainer's machine.

Reference: `bench/harness.py` (paired baseline/candidate, trial JSON), `bench/candidates.py`, `bench/README.md`. Tests live in `tests/` and skip unless the `bench` extra (`pip install -e ".[bench]"`) is installed.

---

## 1. Result comparison / correctness checker (`bench/compare.py`)

**Goal.** One tested function that decides "candidate output equals baseline output" so no speed credit is given for wrong results.

**Interface.**
`compare_tables(con_a, con_b, table, *, ordered_by=None, float_tol=1e-9) -> list[str]` (empty list = equal; otherwise human-readable problems, at most 5 sample differing rows). Plus `compare_dag(con_a, con_b, tables, **kw) -> dict[table, list[str]]`.

**Rules.** Same column names and order, else a "columns differ" problem. Default is multiset equality (duplicate counts matter), NULL equals NULL, NaN equals NaN, floats equal within relative `float_tol`, timestamps/dates/decimals compared exactly. With `ordered_by`, rows must also match in that order, and rows tied on `ordered_by` are compared as multisets (ordering ties are not a difference). Must work without loading both tables into Python lists when large (use `EXCEPT ALL` in DuckDB where both tables live in one connection; fall back to sorted comparison across connections).

**Acceptance.** Unit tests covering: duplicate-count difference, NULL vs empty string, NULL in group keys, float noise below/above tolerance, order ties, column order difference, empty vs empty, one empty. `harness.compare` is replaced by a call to this module in a follow-up PR from the harness owner (do not edit harness.py).

---

## 2. Report formatter (`bench/report.py`)

**Goal.** Turn trial JSON into the requested report: geometric-mean speedup plus improved / unchanged / slower / incorrect / failed counts, every assigned case visible.

**Input.** The JSON written by `harness.py` (`{"trials": [{scenario, seed, status, scan_ratio, time_ratio, baseline:{rows_scanned,statements,seconds}, candidate:{...}, detail}]}`). Make the metric fields generic: also accept `total_slot_ms`, `bytes_processed`, `bytes_billed` (BigQuery) under `baseline`/`candidate`.

**Interface.** `python -m bench.report results.json [--baseline-json other.json] > report.md`; `format_report(trials, metrics=("rows_scanned","seconds",...)) -> str`.

**Output.** Per scenario: trials, faster, same, slower, INCORRECT, failed, unsupported, geomean ratio for each metric computed over correct trials only, min/max. A header line with totals. A section listing every non-correct trial with its detail. With `--baseline-json`, a per-scenario delta against a previous run ("did we improve?") and a regression list (scenario got worse by more than a threshold). Status thresholds are parameters (defaults: faster <0.95, slower >1.05).

**Acceptance.** Snapshot tests on a small hand-written results file; an incorrect trial must never contribute to a geomean; zero-work ratios are clamped rather than crashing.

---

## 3. SSB multi-seed data variants (`bench/ssb_variants.py`)

**Goal.** Correctness must hold across data shapes, not one dataset. Produce deterministic perturbations of the SSB sources loaded by `bench/ssb_data.py`.

**Interface.** `apply_variant(con, name, seed)` mutates the source tables in a DuckDB connection (tables `lineorder, customer, supplier, part, dates`, schema as in `ssb_data.load_sources`). Variants: `clean`, `nulls` (NULLs in measures, dimension attributes, discount), `duplicates` (duplicate dimension rows and duplicate fact rows with distinct `lo_rowid`), `unmatched_keys` (fact rows whose custkey/suppkey/partkey/order date have no dimension row), `empty_partitions` (days with zero rows, a dimension with zero rows), `skewed_keys` (90% of rows on 3 customers), `ties` (many rows with identical sort keys).
`variants.py --list` prints names. Deterministic: same (name, seed) gives identical data.

**Acceptance.** Tests assert each variant creates the property it names (counts of NULLs, duplicates, unmatched keys, etc. above a threshold) and is deterministic. Document how the harness owner can add `--variant` later; do not edit the harness.

---

## 4. ClickBench subset runner (`bench/clickbench/`)

**Goal.** First wide-table runtime benchmark, offline, at tiny scale, as a query-level track (separate from Kingyo's pipeline track).

**Scope.** A generator for a synthetic table with the ClickBench `hits` column names/types for ~30 representative columns (documented as "ClickBench-shaped, synthetic", not the official data) written to Parquet by seed and row count; a file of 12-15 queries copied in spirit from ClickBench covering: count, filtered count, group-by with order/limit, COUNT(DISTINCT), string LIKE, wide `SELECT *` vs projected columns, date-range filter. Each query is a `(id, baseline_sql, candidate_sql_or_None, note)`; where a hand-written equivalent rewrite exists (column pruning, early filter, pre-aggregation) include it as candidate and require `compare_tables` equality. Runner prints rows scanned (DuckDB profiling, as in `Meter` in harness.py) and seconds, paired and order-randomized, repeated.

**Acceptance.** `python -m bench.clickbench.run --rows 200000 --seeds 3` runs offline in under a few minutes; unit test with 5k rows; queries list the original ClickBench query number they are modelled on or "original".

---

## 5. BigQuery-specific micro-suite (`bench/bq_micro/`)

**Goal.** Targeted cases for BigQuery behaviours generic benchmarks miss, defined as data so they can be executed later on a maintainer's machine: column pruning, partition pruning (literal list vs CAST vs IN-subquery), clustering (leading vs non-leading filter, selectivity), joins/shuffle (early filter, valid pre-aggregation, skewed key), nested data (STRUCT/ARRAY/UNNEST), reuse/materialization (build+refresh cost vs repeated compute).

**Interface.** `cases.py` defines `Case(id, family, setup_sql[], baseline_sql, candidate_sql, equivalent: bool, expected_effect, bq_dialect=True)` using GoogleSQL with placeholders `{dataset}` (never a real project). `check_syntax.py` parses every statement with `sqlglot` (dialect `bigquery`). For cases expressible in DuckDB, `offline.py` builds a scaled-down DuckDB table and asserts baseline and candidate return equal results via `compare_tables`.

**Rules.** Follow the project's cost rules: partition filters are literal lists on the raw partition column; staging projects only needed columns. Include at least 3 cases per family, including at least one where the candidate is NOT equivalent (a trap) so the checker has something to catch.

**Acceptance.** All statements parse as BigQuery SQL; offline equivalence passes for equivalent cases and fails for the trap cases; a markdown table of cases is generated for review. No BigQuery client import anywhere in this package.

---

## 6. BigQuery backend and job-stats collector (`bench/bq_backend.py`) — later, needs maintainer

**Goal.** Run a paired baseline/candidate on BigQuery and record work, not just time. Built and unit-tested against a fake client only; Walter runs it from their machine after reviewing.

**Interface.** `BigQueryBackend(project, dataset, *, max_bytes_billed, labels)`: project passed explicitly (no default-project discovery), query cache disabled, `maximum_bytes_billed` enforced on every job, dry-run mode that returns `total_bytes_processed` only. After each job collect `total_bytes_processed`, `total_bytes_billed`, `total_slot_ms`, elapsed time, and from the job's query plan the shuffle and spill figures; aggregate per trial into the same `baseline`/`candidate` dicts the report formatter reads (issue 2).

**Rules.** Writes only to the named dataset; refuse any statement outside it; refuse to run without an explicit `--execute` flag; default is dry-run. Never log or commit identifiers.

**Acceptance.** Tests with a fake client showing: dry-run by default, byte cap passed on every job, project never inferred, stats aggregation correct. Label `needs-walter` until reviewed.

---

Kept by the pipeline thread (not for Sol): `harness.py`, `scenarios.py`, `ssb_repo/`, orchestration of Kingyo-specific scenarios (unchanged sources, incremental changes, schema and model changes), and integration of the pieces above. Suggested order for Sol: 1 and 2 first (small, unblock everything), then 3, 4 and 5 in parallel, 6 last.

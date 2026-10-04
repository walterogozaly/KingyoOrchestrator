# KingyoOrchestrator

A Python workspace for building an observe-first orchestrator around BigQuery,
Dataform, and KumoSQL. Python is the initial implementation choice because the
adjacent tooling and metadata clients already use it.

The starter includes packaging, a local configuration validator, an offline
metadata interface with a fake reader, offline day-partition resolution, local
snapshot persistence and comparison, compiled Dataform graph ingestion, tests,
and CI.
Cloud integrations, polling, dependency graph planning, and execution are future work.
All current commands run locally without credentials or cloud calls.
The standalone [offline impact planner](docs/planning.md) proposes affected actions
in deterministic build order from a caller-supplied dependency graph.
The [small-dimension fingerprint API](docs/dimension-fingerprints.md) renders
content-check SQL and compares caller-supplied results to suppress audit-only signals.

## Start working

Requires Python 3.11 or newer. From this folder in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item config/kingyo.example.toml config/kingyo.toml
.\.venv\Scripts\python.exe -m kingyo_orchestrator --version
.\.venv\Scripts\python.exe -m kingyo_orchestrator check-config config/kingyo.toml
```

Edit the local configuration with your own project and location. The example
contains placeholders. The `kingyo` console command is also available inside
the virtual environment; activation is optional when using the explicit paths.

On macOS/Linux, use `.venv/bin/python` in place of `.\.venv\Scripts\python.exe`
and `cp` in place of `Copy-Item`.

## Layout

```text
src/kingyo_orchestrator/  Python package, CLI, and configuration
tests/                   Offline configuration, CLI, metadata, partition, state, and graph checks
config/                  Public example settings; local settings are ignored
docs/                    Architecture and initial work sequence
.github/workflows/       Lint, tests, and package build checks
```

`core/` contains pure change comparison and offline partition decisions;
`adapters/` renders discovery SQL and literal DATE/TIMESTAMP partition filters as
strings, and `state/` persists local checkpoints.
See [architecture](docs/architecture.md), the [metadata interface](docs/metadata.md),
the [partition resolver and cost limits](docs/partition-resolution.md),
the [snapshot state format](docs/state.md), the [column relevance rules](docs/column-relevance.md), the [compiled graph interface](docs/compiled-graph.md),
and the [initial roadmap](docs/roadmap.md).

## Incremental prototype

`src/kingyo_orchestrator/incremental/` is the offline incremental-orchestration prototype: it reads plain
(non-incremental) SQLX, classifies each dependency edge (aligned partitions, keyed, outer-join, column-aware
relevance, or full refresh), and runs the unchanged SQL incrementally against a **local DuckDB** database on each
scheduled wake-up. It makes no cloud calls. The supported SQL style is in
[docs/incremental-supported-sql.md](docs/incremental-supported-sql.md). Install with
`pip install -e ".[incremental]"` and try `python -m kingyo_orchestrator.incremental.demo`.

## Column relevance

`core/column_usage.py` answers, per downstream consumer, whether a changed column is read at all, so an
audit-only change can be a no-op for one action and required for another. Column usage analysis for plain
BigQuery SQL is behind an optional extra:

```console
pip install -e ".[lineage]"
```

See [column relevance and its conservative fallback](docs/column-relevance.md).

## Benchmarks

`bench/` holds an offline benchmark harness (SSB-derived Dataform pipeline on DuckDB, paired baseline/candidate
runs, correctness before speed). See [benchmark commands](bench/README.md); install with `pip install -e ".[bench]"`.
The standard-library report command, `python -m bench.report results.json`, summarizes saved trials
and can compare a previous run without opening databases or contacting cloud services.

## Verify changes

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest -q -n auto
.\.venv\Scripts\python.exe -m build
```

The suite is safe to run in parallel (`-n auto` uses pytest-xdist from the `dev` extra); drop the flag to run serially.

## Local information

This repository is public. Private handoff notes, machine paths, credentials,
and real infrastructure identifiers belong in ignored local files. Existing
handoff files remain in this workspace and are excluded from Git, along with
`docs/local/`, `config/kingyo.toml`, `.env*`, `var/`, and `artifacts/`.

Future cloud adapters should use the provider's normal credential mechanisms,
require an explicit project, and start with metadata reads and SQL dry runs.
Execution, writes, and IAM changes require a separate implementation decision.

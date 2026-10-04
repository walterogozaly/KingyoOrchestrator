# KingyoOrchestrator

A Python workspace for building an observe-first orchestrator around BigQuery,
Dataform, and KumoSQL. Python is the initial implementation choice because the
adjacent tooling and metadata clients already use it.

The starter includes packaging, a local configuration validator, an offline
metadata interface with a fake reader, offline day-partition resolution, tests,
and CI.
Cloud integrations, polling, and execution are future work.
All current commands run locally without credentials or cloud calls.
The standalone [offline impact planner](docs/planning.md) proposes affected actions
in deterministic build order from a caller-supplied dependency graph.

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
tests/                   Offline configuration, CLI, metadata, and partition checks
config/                  Public example settings; local settings are ignored
docs/                    Architecture and initial work sequence
.github/workflows/       Lint, tests, and package build checks
```

`core/` contains offline partition decisions, and `adapters/` renders discovery
SQL as strings. Future integrations can extend adapters; checkpoint persistence
will live under `state/`.
See [architecture](docs/architecture.md), the [metadata interface](docs/metadata.md),
the [partition resolver and cost limits](docs/partition-resolution.md), and the
[initial roadmap](docs/roadmap.md).

## Verify changes

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m build
```

## Local information

This repository is public. Private handoff notes, machine paths, credentials,
and real infrastructure identifiers belong in ignored local files. Existing
handoff files remain in this workspace and are excluded from Git, along with
`docs/local/`, `config/kingyo.toml`, `.env*`, `var/`, and `artifacts/`.

Future cloud adapters should use the provider's normal credential mechanisms,
require an explicit project, and start with metadata reads and SQL dry runs.
Execution, writes, and IAM changes require a separate implementation decision.

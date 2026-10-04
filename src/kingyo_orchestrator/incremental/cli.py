"""Cron-friendly entry points. State lives in the warehouse, so every invocation is independent.

  python -m kingyo_orchestrator.incremental.cli --db wh.duckdb --repo sample_repo signal orders 2026-10-03 --observed 2026-10-04T21:10
  python -m kingyo_orchestrator.incremental.cli --db wh.duckdb --repo sample_repo run --settle-minutes 15 --budget-seconds 600
  python -m kingyo_orchestrator.incremental.cli --db wh.duckdb --repo sample_repo status
`signal` without --observed means "just now". Schedule `run` on any interval; it no-ops when nothing is dirty.
"""

import argparse
from datetime import datetime, timedelta

import duckdb

from .analysis import analyze_edge
from .executor import ALL, Orchestrator, Policy
from .graph import Dag
from .sqlx import load_repo

ADVICE = [  # (substring of the analyzer's reason, what to change)
    (
        "target is unpartitioned",
        "add bigquery.partitionBy to this model (fine to leave if the table is small)",
    ),
    (
        "GROUP BY does not include",
        "group by the partition column, or by one key column that you also select (pattern C)",
    ),
    (
        "window function not partitioned",
        "PARTITION BY the partition column or one selected key column (pattern C)",
    ),
    (
        "LIMIT",
        "avoid LIMIT/OFFSET; for latest-per-key use row_number() over (partition by key) in a subquery",
    ),
    (
        "nullable side",
        "join the changed table with a single equality, in a plain row-level SELECT (pattern D)",
    ),
    (
        "more than once",
        "read the changed table once (no self-join, no CTE over it referenced twice)",
    ),
    (
        "subquery expression",
        "move the subquery over the changed table into a join or a separate model",
    ),
    (
        "cannot resolve partition column",
        "select the partition column straight from the parent (no struct/aggregate on it), or key the model (C)",
    ),
    (
        "global aggregate",
        "add a GROUP BY on a key or the partition column, or keep the model small and unpartitioned",
    ),
    ("depends on columns outside", "derive the partition column from the changed parent only"),
]


def check(orch) -> str:
    out, cols = [], orch.columns()
    if not cols:
        out.append(
            "note: no warehouse tables found (--db); keyed and outer-join patterns need column names, so they show as full"
        )
    for n in orch.dag.order:
        m = orch.dag.models[n]
        for p in m.deps:
            s = analyze_edge(m, orch.dag.models[p], cols)
            line = f"{p:>22} -> {n:<24} {s.kind:<15}"
            if s.kind != "full" and not orch.dag.models[p].partition_expr:
                line += f" but {p} is unpartitioned, so its signals are ALL and {n} is rebuilt\n{'':>66}fix: declare partitionBy on {p}"
            if s.kind == "full":
                tip = next((t for k, t in ADVICE if k in s.reason), "")
                line += f" {s.reason}" + (f"\n{'':>66}fix: {tip}" if tip else "")
            out.append(line)
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--repo", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("signal")
    s.add_argument("table")
    s.add_argument("partitions", nargs="*", help="partition values; omit for ALL")
    s.add_argument("--observed", help="ISO time the data landed (default now)")
    s.add_argument(
        "--columns", nargs="+", help="only these columns changed in existing rows (no new rows)"
    )
    r = sub.add_parser("run")
    r.add_argument("--settle-minutes", type=float, default=15)
    r.add_argument("--budget-seconds", type=float)
    r.add_argument("--full-refresh-ratio", type=float, default=0.5)
    sub.add_parser("status")
    sub.add_parser(
        "check",
        help="which incremental pattern each dependency gets, and what to change (see SUPPORTED_SQL.md)",
    )
    a = ap.parse_args(argv)

    orch = Orchestrator(Dag(load_repo(a.repo)), duckdb.connect(a.db))
    if a.cmd == "signal":
        when = datetime.fromisoformat(a.observed) if a.observed else datetime.now()
        orch.signal(a.table, a.partitions or ALL, when, columns=a.columns)
    elif a.cmd == "run":
        rep = orch.run_once(
            policy=Policy(
                timedelta(minutes=a.settle_minutes), a.full_refresh_ratio, a.budget_seconds
            )
        )
        print("\n".join(rep.steps))
    elif a.cmd == "check":
        print(check(orch))
    else:
        for row in orch.status():
            print(*row)


if __name__ == "__main__":
    main()

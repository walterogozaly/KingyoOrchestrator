"""Candidates under test. A candidate receives a metered connection (every execute() is counted), the repo path,
the loaded models and the external signals [(table, [partition values] | "ALL"[, [changed columns]])], and must
bring all derived models up to date with the mutated sources.

  full-rebuild              control: does what the baseline does (ratios must be ~1.0; proves the harness is neutral)
  kingyo-prototype          the in-repo incremental prototype (kingyo_orchestrator.incremental), column hints ignored
  kingyo-prototype-columns  same, but passes the optional changed-columns hint when a scenario provides one
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pipeline

SRC = Path(__file__).resolve().parents[1] / "src"  # repo layout: run without installing the package


class FullRebuild:
    capabilities: frozenset = frozenset({"signal"})

    def run(self, con, repo, models, signals):
        pipeline.build_all(con, models)


class KingyoPrototype:
    """The in-repo incremental prototype. Signals are (table, partitions) or (table, partitions, columns);
    `use_columns` passes the optional changed-column hint through (column-aware relevance)."""

    capabilities: frozenset = frozenset({"signal"})
    use_columns = False

    def run(self, con, repo, models, signals):
        if str(SRC) not in sys.path:
            sys.path.insert(0, str(SRC))
        from kingyo_orchestrator.incremental.executor import Orchestrator, Policy
        from kingyo_orchestrator.incremental.graph import Dag
        from kingyo_orchestrator.incremental.sqlx import load_repo

        now = datetime(2026, 10, 4, 12)
        o = Orchestrator(Dag(load_repo(repo)), con)
        for table, parts, *rest in signals:
            cols = rest[0] if rest and self.use_columns else None
            o.signal(table, parts, now - timedelta(days=1), columns=cols)
        o.run_once(now, Policy(settle=timedelta(0)))


class KingyoPrototypeColumns(KingyoPrototype):
    use_columns = True


CANDIDATES = {
    "full-rebuild": FullRebuild,
    "kingyo-prototype": KingyoPrototype,
    "kingyo-prototype-columns": KingyoPrototypeColumns,
}

"""Candidates under test. A candidate receives a metered connection (every execute() is counted), the repo path,
the loaded models and the external signals [(table, [partition values] | "ALL")], and must bring all derived
models up to date with the mutated sources.

  full-rebuild        control: does what the baseline does (ratios must be ~1.0; proves the harness is neutral)
  kingyo-prototype    the incremental prototype; needs KINGYO_PROTOTYPE_PATH to point at the directory that contains
                      its `kingyo/` package (it is not part of this repository yet)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pipeline


class FullRebuild:
    capabilities: frozenset = frozenset({"signal"})

    def run(self, con, repo, models, signals):
        pipeline.build_all(con, models)


class KingyoPrototype:
    capabilities: frozenset = frozenset({"signal"})

    def run(self, con, repo, models, signals):
        path = os.environ.get("KINGYO_PROTOTYPE_PATH")
        if not path:
            raise RuntimeError(
                "set KINGYO_PROTOTYPE_PATH to the directory containing the prototype's kingyo/ package"
            )
        sys.path.insert(0, path)
        from kingyo.executor import Orchestrator, Policy
        from kingyo.graph import Dag
        from kingyo.sqlx import load_repo

        now = datetime(2026, 10, 4, 12)
        o = Orchestrator(Dag(load_repo(repo)), con)
        for table, parts in signals:
            o.signal(table, parts, now - timedelta(days=1))
        o.run_once(now, Policy(settle=timedelta(0)))


CANDIDATES = {"full-rebuild": FullRebuild, "kingyo-prototype": KingyoPrototype}

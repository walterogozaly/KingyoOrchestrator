"""Column-level lineage: which source columns a consumer actually reads.

    from kingyo_orchestrator.lineage import relevance, SqlglotColumnUsage

    answers = relevance(graph, changed_table, ["loaded_at"])
    for child, answer in answers.items():
        if answer.no_op:
            ...  # this child does not read the change; skipping it is safe

`SqlglotColumnUsage` needs the optional `lineage` extra; any other `ColumnUsageSource`
can be passed to `relevance` instead, and KumoSQL's own lineage is expected to replace it later.
"""

from .relevance import ChildRelevance, relevance
from .sqlglot_usage import SqlglotColumnUsage
from .types import ColumnUsage, ColumnUsageSource, LineageDependencyError, Queryable

__all__ = [
    "ChildRelevance",
    "ColumnUsage",
    "ColumnUsageSource",
    "LineageDependencyError",
    "Queryable",
    "SqlglotColumnUsage",
    "relevance",
]

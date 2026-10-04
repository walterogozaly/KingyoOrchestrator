"""Column-usage analysis behind an optional dependency.

`SqlglotColumnUsageSource` needs the `lineage` extra:

    pip install 'kingyo-orchestrator[lineage]'

Importing this package does not import sqlglot; only constructing the source and
calling it does, and a missing dependency raises `LineageDependencyError` with the
install command. KumoSQL has its own lineage and may replace this later, which is
why everything above `core/column_usage.py` talks to the `ColumnUsageSource`
protocol instead of to sqlglot.
"""

from .sqlglot_source import (
    ColumnCatalog,
    LineageDependencyError,
    SqlglotColumnUsageSource,
    StaticColumnCatalog,
)

__all__ = [
    "ColumnCatalog",
    "LineageDependencyError",
    "SqlglotColumnUsageSource",
    "StaticColumnCatalog",
]

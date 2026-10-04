"""Column-relevance types: which source columns one consumer actually reads.

The interface is deliberately small so the lineage source can be swapped. KumoSQL has
its own lineage and may replace the sqlglot implementation later; only `ColumnUsage`
crosses that boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, runtime_checkable


class LineageDependencyError(RuntimeError):
    """The optional lineage extra is not installed. Install it, or pass a different source."""


@dataclass(frozen=True)
class ColumnUsage:
    """The source columns one consumer reads, keyed by source table.

    `used` maps a normalised SQL table reference (lowercase, dotted, no quoting, e.g.
    `p.d.orders`) to the set of column names that consumer reads from it.

    `unresolved` is the conservative fallback: the consumer's column usage could not be
    resolved, so every column must be treated as used and `reason` says why. A false
    "used" only costs a rebuild; a false "unused" silently skips needed work, so this is
    never inferred optimistically.
    """

    used: Mapping[str, frozenset[str]] = field(default_factory=dict)
    unresolved: bool = False
    reason: str = ""

    def __post_init__(self) -> None:
        normalised = {key: frozenset(value) for key, value in self.used.items()}
        if self.unresolved and not self.reason:
            raise ValueError("an unresolved ColumnUsage needs a reason")
        object.__setattr__(self, "used", MappingProxyType(normalised))

    @classmethod
    def resolved(cls, used: Mapping[str, frozenset[str]] | None = None) -> ColumnUsage:
        return cls(used or {})

    @classmethod
    def unresolved_fallback(cls, reason: str) -> ColumnUsage:
        """Every column counts as used; `reason` must say why the real usage is unknown."""
        return cls({}, unresolved=True, reason=reason)

    def columns_for(self, *tables: str) -> frozenset[str]:
        """Columns read from any of `tables`, comparing on the last component of the key."""
        wanted = {_bare(table) for table in tables}
        found: set[str] = set()
        for table, columns in self.used.items():
            if _bare(table) in wanted:
                found |= columns
        return frozenset(found)

    def reads(self, table: str) -> bool:
        return bool(self.columns_for(table))


def _bare(table: str) -> str:
    return table.rsplit(".", 1)[-1].strip('`"').lower()


@runtime_checkable
class Queryable(Protocol):
    """The part of an action the lineage source needs: the compiled query text."""

    query: str | None


@runtime_checkable
class ColumnUsageSource(Protocol):
    """Where column usage comes from. Swap this to change lineage backends."""

    def column_usage(self, action: Queryable) -> ColumnUsage:
        """Columns `action` reads, or the conservative `unresolved` fallback."""
        ...

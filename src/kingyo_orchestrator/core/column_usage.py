"""Per-consumer column relevance: is a changed column read downstream at all?

Pure decisions only. This module never executes SQL, never parses SQL, and never
imports the optional lineage dependency: the analysis behind
`ColumnUsageSource.column_usage` lives in `kingyo_orchestrator.lineage`.

The question is asked per consumer, never once for the whole graph. A column that
is an audit column for one child is often the join key for another, so one change
can be relevant to one child and a no-op for the next. The result is keyed by
child for exactly that reason.

Conservatism is the point: a wrong `relevant` only costs a rebuild, while a wrong
`no_op` silently skips work that was needed. Nothing here ever guesses toward
`no_op`. When a consumer's column usage cannot be resolved, every column is
treated as used and the reason is reported.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

_NO_SOURCE_REASON = "no column usage source was configured, so every column is treated as used"


def _tail(reference: str) -> str:
    """The table name of a graph id (`project.dataset.table`) or SQL reference (`table`)."""
    return reference.rsplit(".", 1)[-1].casefold()


@dataclass(frozen=True)
class ColumnUsage:
    """The source columns one consumer reads, grouped by source table reference.

    `by_table` keys are the references as written in the consumer's SQL. They are
    matched to a graph id by table name only, so a query reading
    `other_project.dataset_a.orders` counts as reading `project_x.dataset_a.orders`.
    That direction is deliberate: it can only add a rebuild, never remove one.

    `unresolved` means usage could not be computed for this consumer. Every column
    of every source must then be treated as used, and `reason` says why.
    """

    by_table: Mapping[str, frozenset[str]] = field(default_factory=dict)
    unresolved: bool = False
    reason: str = ""

    @classmethod
    def all_used(cls, reason: str) -> ColumnUsage:
        """Usage that cannot be resolved: treat every column as used."""
        return cls(by_table={}, unresolved=True, reason=reason)

    def reads_table(self, table: str) -> bool:
        """Whether the analysis saw `table` at all, matched on table name.

        False here can mean the consumer genuinely reads only other tables, or that
        the consumer's SQL names the table differently from the graph. The caller
        cannot tell the two apart from this answer alone, so a graph edge to a table
        this analysis never saw must not be treated as a no-op.
        """
        return any(_tail(reference) == _tail(table) for reference in self.by_table)

    def uses(self, table: str, column: str) -> bool | None:
        """Whether `column` of `table` is read, or None when the consumer is unresolved.

        Column and table names compare case-insensitively, as BigQuery does.
        """
        if self.unresolved:
            return None
        key = column.casefold()
        return any(
            _tail(reference) == _tail(table) and key in columns
            for reference, columns in self.by_table.items()
        )

    def matching_columns(self, table: str, columns: Iterable[str]) -> tuple[str, ...]:
        """The caller's `columns` that this consumer reads, in the caller's spelling."""
        return tuple(column for column in columns if self.uses(table, column) is True)


class ActionLike(Protocol):
    """The minimum a consumer must expose to be analyzed.

    A compiled graph action from `kingyo_orchestrator.graph` satisfies this through a
    thin adapter: its `id` becomes `str(action.id)` and its `ActionId` dependencies
    become strings. See `docs/column-relevance.md`.
    """

    id: str
    query: str | None
    depends_on: tuple[str, ...]


class ColumnUsageSource(Protocol):
    """Pluggable column-usage analysis, so lineage can be swapped or replaced."""

    def column_usage(self, action: ActionLike) -> ColumnUsage:
        """Columns `action` reads, or `ColumnUsage.all_used(reason)`."""
        ...


@dataclass(frozen=True)
class ConsumerAction:
    """A minimal consumer: the seam that stands in for a graph action."""

    id: str
    query: str | None = None
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConsumerRelevance:
    """Whether one changed column matters to one child action.

    `relevant` carries the changed columns and keys that child actually reads.
    `no_op` means the child reads none of them, so it can be skipped.
    """

    consumer: str
    relevant: bool
    columns: tuple[str, ...]
    keys: tuple[str, ...]
    reason: str

    @property
    def verdict(self) -> str:
        return "relevant" if self.relevant else "no_op"


def _consumers(
    graph: Mapping[str, ActionLike], changed_table: str
) -> tuple[tuple[str, ActionLike], ...]:
    """Direct consumers of `changed_table`, in mapping-key order.

    The mapping key is the action id, so callers can build the mapping straight from
    a graph's node list.
    """
    return tuple(
        (identifier, action)
        for identifier, action in sorted(graph.items())
        if changed_table in tuple(action.depends_on)
    )


def relevance(
    graph: Mapping[str, ActionLike],
    changed_table: str,
    changed_columns: Sequence[str],
    changed_keys: Sequence[str] | None = None,
    *,
    source: ColumnUsageSource | None = None,
) -> dict[str, ConsumerRelevance]:
    """Per-child verdict for a change to `changed_table`.

    `changed_columns` are the columns whose content changed. `changed_keys`, when
    supplied, are changed key values from the fingerprint work: a child that reads
    a changed key is relevant even if it reads none of the changed columns, because
    its rows can move.

    Children whose usage cannot be resolved are always `relevant`, never `no_op`.
    Without a `source` every child is treated that way: the caller has not supplied
    any way to know what is read.
    """
    if not isinstance(graph, Mapping):
        raise TypeError("graph must be a mapping of action id to action")
    changed = tuple(changed_columns)
    keys = tuple(changed_keys) if changed_keys is not None else ()

    results: dict[str, ConsumerRelevance] = {}
    for identifier, action in _consumers(graph, changed_table):
        if source is None:
            results[identifier] = ConsumerRelevance(
                consumer=identifier,
                relevant=True,
                columns=(),
                keys=(),
                reason=_NO_SOURCE_REASON,
            )
            continue
        usage = source.column_usage(action)
        if usage.unresolved:
            results[identifier] = ConsumerRelevance(
                consumer=identifier,
                relevant=True,
                columns=(),
                keys=(),
                reason=f"usage unresolved, so every column counts: {usage.reason}",
            )
            continue
        if not usage.reads_table(changed_table):
            seen = ", ".join(sorted(usage.by_table)) or "no source table"
            results[identifier] = ConsumerRelevance(
                consumer=identifier,
                relevant=True,
                columns=(),
                keys=(),
                reason=(
                    f"the analysis did not see {changed_table} in this query (references: {seen}), "
                    "so its columns cannot be judged unused"
                ),
            )
            continue
        hit_columns = usage.matching_columns(changed_table, changed)
        hit_keys = usage.matching_columns(changed_table, keys)
        relevant = bool(hit_columns or hit_keys)
        results[identifier] = ConsumerRelevance(
            consumer=identifier,
            relevant=relevant,
            columns=hit_columns,
            keys=hit_keys,
            reason=_reason(relevant, hit_columns, hit_keys, changed, keys),
        )
    return results


def _reason(
    relevant: bool,
    hit_columns: tuple[str, ...],
    hit_keys: tuple[str, ...],
    changed: tuple[str, ...],
    keys: tuple[str, ...],
) -> str:
    if relevant:
        read = ", ".join([*hit_columns, *(f"key {key}" for key in hit_keys)])
        return f"reads changed {read}"
    listed = ", ".join(changed) or "none"
    suffix = f" or changed keys ({', '.join(keys)})" if keys else ""
    return f"reads none of the changed columns ({listed}){suffix}"

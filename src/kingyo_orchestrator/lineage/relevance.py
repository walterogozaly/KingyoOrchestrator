"""Per-consumer relevance: is a changed column used downstream at all?

The universal question is not "is this the audit column?" but "does any column that
changed actually get read downstream?". One change can be relevant to one child and a
no-op for another, so the answer is keyed by child and never a single global verdict.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from ..graph.types import Action, ActionId, DependencyGraph
from .types import ColumnUsage, ColumnUsageSource, LineageDependencyError


@dataclass(frozen=True)
class ChildRelevance:
    """Whether one downstream action is affected by a change to its parent."""

    child: ActionId
    relevant: bool
    reason: str
    columns: tuple[str, ...] = ()
    keys: tuple[str, ...] = ()
    unresolved: bool = False

    @property
    def no_op(self) -> bool:
        return not self.relevant


def _table_names(changed_table: ActionId) -> tuple[str, ...]:
    """Every spelling a query might use for this action: full, schema-qualified, bare."""
    return (
        f"{changed_table.database}.{changed_table.schema}.{changed_table.name}",
        f"{changed_table.schema}.{changed_table.name}",
        changed_table.name,
    )


def relevance(
    graph: DependencyGraph,
    changed_table: ActionId,
    changed_columns: Iterable[str],
    changed_keys: Sequence[str] | None = None,
    *,
    source: ColumnUsageSource | None = None,
) -> dict[ActionId, ChildRelevance]:
    """For every direct child of `changed_table`, decide whether the change matters to it.

    A child is `relevant` when it reads at least one changed column of the parent, or
    when the caller supplied `changed_keys` and the child reads one of them. A child whose
    usage cannot be resolved is always `relevant`, because a false `no_op` would silently
    skip needed work. A child that reads none of them is a `no_op` and may be skipped.
    """
    if source is None:
        from .sqlglot_usage import SqlglotColumnUsage

        source = SqlglotColumnUsage()
    children = graph.children(
        changed_table
    )  # raises UnknownActionError for a node outside the graph
    by_id = {action.id: action for action in graph.actions}
    wanted = _table_names(changed_table)
    columns = tuple(sorted({str(column) for column in changed_columns}))
    keys = tuple(sorted({str(key) for key in changed_keys})) if changed_keys else ()
    return {
        child: _for_child(child, by_id[child], source, wanted, columns, keys) for child in children
    }


def _for_child(
    child: ActionId,
    action: Action,
    lineage: ColumnUsageSource,
    wanted: tuple[str, ...],
    columns: tuple[str, ...],
    keys: tuple[str, ...],
) -> ChildRelevance:
    usage = _usage(action, lineage)
    if usage.unresolved:
        return ChildRelevance(
            child,
            relevant=True,
            reason=f"usage unresolved ({usage.reason}); treating every column as used",
            unresolved=True,
        )
    read = usage.columns_for(*wanted)
    hit_columns = tuple(column for column in columns if column in read)
    hit_keys = tuple(key for key in keys if key in read)
    if hit_columns or hit_keys:
        return ChildRelevance(
            child,
            relevant=True,
            reason=f"reads changed {', '.join(hit_columns + hit_keys)}",
            columns=hit_columns,
            keys=hit_keys,
        )
    if not read:
        reason = f"reads no column of {wanted[-1]}"
    elif columns:
        reason = f"reads {sorted(read)} of {wanted[-1]}, none of {list(columns)} changed"
    else:
        reason = f"reads {sorted(read)} of {wanted[-1]} but no column changed"
    if keys:
        reason += f"; checked keys {list(keys)}"
    return ChildRelevance(child, relevant=False, reason=reason)


def _usage(action: Action, lineage: ColumnUsageSource) -> ColumnUsage:
    try:
        return lineage.column_usage(action)
    except LineageDependencyError:
        raise  # a missing extra is a setup problem, not an unresolved column usage
    except Exception as exc:  # noqa: BLE001 - a lineage backend must never break the planner
        return ColumnUsage.unresolved_fallback(f"lineage backend failed ({type(exc).__name__})")

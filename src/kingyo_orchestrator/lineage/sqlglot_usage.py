"""sqlglot-backed column usage for plain BigQuery SQL.

sqlglot is imported lazily so the rest of Kingyo keeps working without it; install
the optional extra with `pip install -e ".[lineage]"`.

Attribution is deliberately one hop. A column reference is attributed to a source
table only when the scope it belongs to resolves unambiguously to a physical table.
A reference that passes through a CTE or subquery is not attributed there, because
that scope is traversed in its own right and its body already names the real source
columns. Anything that cannot be resolved returns the conservative fallback: every
column is treated as used, with a reason.

Over-attribution is allowed (a column credited to a table it does not come from)
because it only ever produces a false `relevant`, which costs a rebuild. The
opposite mistake, under-attribution, would report a `no_op` and silently skip work.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .types import ColumnUsage, LineageDependencyError, Queryable

#: Marker for text that is not plain SQL, e.g. uncompiled templating or a Dataform JS block.
_DYNAMIC_MARKERS = ("${", "self()", "{{", "}}")
_INSTALL_HINT = "column lineage needs sqlglot; install the extra with pip install -e '.[lineage]'"


def _sqlglot():
    try:
        import sqlglot
        from sqlglot.optimizer.scope import Scope, traverse_scope
    except ImportError as exc:  # pragma: no cover - exercised only without the extra installed
        raise LineageDependencyError(_INSTALL_HINT) from exc
    return sqlglot, Scope, traverse_scope


def _dynamic_reason(query: str) -> str:
    for marker in _DYNAMIC_MARKERS:
        if marker in query:
            return f"query is not plain SQL after compile (found {marker!r})"
    return ""


def _clean(text: str | None) -> str:
    return str(text or "").strip('`"').lower()


def _key(node) -> str:
    parts = [part for part in (node.catalog, node.db, node.name) if part]
    return ".".join(parts).strip('`"').lower()


def _spellings(node, alias: str) -> tuple[str, ...]:
    """Every way this table may be named: alias, bare name, schema.name, db.schema.name."""
    parts = [part for part in (node.catalog, node.db, node.name) if part]
    names = [alias, *parts, *[".".join(parts[i:]) for i in range(1, len(parts))]]
    return tuple(dict.fromkeys(name for name in map(_clean, names) if name))


def _unique(nodes):
    nodes = list(nodes or ())
    return nodes[0] if len(nodes) == 1 else None


class SqlglotColumnUsage:
    """`ColumnUsageSource` over plain BigQuery SQL. Stateless and safe to share."""

    dialect = "bigquery"

    def column_usage(self, action: Queryable) -> ColumnUsage:
        query = getattr(action, "query", None)
        if not isinstance(query, str) or not query.strip():
            return ColumnUsage.unresolved_fallback("action has no query text")
        marker = _dynamic_reason(query)
        if marker:
            return ColumnUsage.unresolved_fallback(marker)

        sqlglot, Scope, traverse_scope = _sqlglot()
        exp = sqlglot.exp
        try:
            tree = sqlglot.parse_one(query, dialect=self.dialect)
        except Exception as exc:  # noqa: BLE001 - any parse failure is a fallback, not a crash
            return ColumnUsage.unresolved_fallback(
                f"query is not parseable as BigQuery SQL ({type(exc).__name__})"
            )

        scopes = list(traverse_scope(tree))
        aliases = _all_aliases(scopes, Scope)
        used: dict[str, set[str]] = defaultdict(set)
        for scope in scopes:
            problem = _star_problem(scope, exp, Scope)
            if problem:
                return ColumnUsage.unresolved_fallback(problem)
            problem = _attribute(scope, used, aliases, exp, Scope)
            if problem:
                return ColumnUsage.unresolved_fallback(problem)
        return ColumnUsage.resolved(used)


def _physical(scope, Scope) -> dict[str, Any]:
    """alias -> table node, for sources that are physical tables rather than CTEs or subqueries."""
    return {
        alias: node
        for alias, (node, source) in scope.selected_sources.items()
        if not isinstance(source, Scope)
    }


def _by_qualifier(scope, Scope) -> dict[str, list]:
    """Every accepted spelling of a physical source -> the table nodes it can mean."""
    index: dict[str, list] = defaultdict(list)
    for alias, node in _physical(scope, Scope).items():
        for spelling in _spellings(node, alias):
            index[spelling].append(node)
    return index


def _all_aliases(scopes, Scope) -> dict[str, list]:
    """Qualifiers seen anywhere in the query, for correlated references to an outer scope."""
    index: dict[str, list] = defaultdict(list)
    for scope in scopes:
        for alias, node in _physical(scope, Scope).items():
            for spelling in _spellings(node, alias):
                if node not in index[spelling]:
                    index[spelling].append(node)
    return index


def _owning_select(node, exp):
    while node is not None:
        if isinstance(node, exp.Select):
            return node
        node = node.parent
    return None


def _own_columns(scope, exp) -> list:
    """Columns this scope owns: every `exp.Column` inside its own SELECT.

    `scope.columns` is not enough, it misses columns in clauses such as `HAVING`, and every nested
    select has a scope of its own with its own sources.
    """
    return [
        column
        for column in scope.expression.find_all(exp.Column)
        if not column.is_star and _owning_select(column, exp) is scope.expression
    ]


def _using_columns(scope, exp) -> list[str]:
    """Column names of `JOIN ... USING (...)`, which sqlglot keeps as identifiers, not columns."""
    return [
        identifier.name
        for join in scope.expression.find_all(exp.Join)
        if _owning_select(join, exp) is scope.expression
        for identifier in join.args.get("using") or ()
        if identifier.name
    ]


def _output_names(scope, exp) -> set[str]:
    return {select.alias_or_name for select in scope.expression.selects if select.alias_or_name}


def _may_name_an_alias(column, exp) -> bool:
    """True in the clauses where a bare name can be an output alias rather than a source column."""
    node = column.parent
    while node is not None:
        if isinstance(node, (exp.Qualify, exp.Order)):
            return True
        node = node.parent
    return False


def _stars(scope, exp):
    for projection in scope.expression.selects:
        if isinstance(projection, exp.Star):
            yield "", projection
        elif isinstance(projection, exp.Column) and projection.is_star:
            yield _clean(projection.table), projection


def _star_problem(scope, exp, Scope) -> str:
    """`SELECT *` on a physical table cannot be enumerated, so it is never a no_op."""
    physical = _physical(scope, Scope)
    names = sorted(_key(node) for node in physical.values())
    stars = [qualifier for qualifier, _ in _stars(scope, exp)] + [
        _clean(star.table)
        for star in scope.stars
        if _owning_select(star, exp) is not scope.expression
    ]
    qualifier_index = _by_qualifier(scope, Scope)
    for qualifier in stars:
        if not qualifier:
            if physical:
                return f"SELECT * in a query reading {names} cannot be enumerated"
            continue
        node = _unique(qualifier_index.get(qualifier))
        if node is None:
            if qualifier not in {_clean(alias) for alias in scope.selected_sources}:
                return f"unresolvable table alias {qualifier!r} in SELECT {qualifier}.*"
            continue
        return f"SELECT {qualifier}.* on {_key(node)} cannot be enumerated"
    return ""


def _attribute(scope, used: dict[str, set[str]], aliases: dict[str, list], exp, Scope) -> str:
    physical = _physical(scope, Scope)
    qualifier_index = _by_qualifier(scope, Scope)
    outputs = _output_names(scope, exp)
    for column in _own_columns(scope, exp):
        name = column.name
        if not name:
            return "a column reference has no resolvable name"
        qualifier = _clean(column.table)
        database = _clean(column.db)
        if qualifier:
            node = _resolve(qualifier, database, physical, qualifier_index, aliases)
            if node is None:
                if qualifier not in {_clean(alias) for alias in scope.selected_sources}:
                    return f"unresolvable table alias {qualifier!r}"
                continue  # a CTE or subquery alias: that scope is traversed separately
            used[_key(node)].add(name)
        elif name in outputs and _may_name_an_alias(column, exp):
            continue  # `QUALIFY rn = 1` / `ORDER BY t` naming this select's own output alias
        elif len(physical) == 1:
            # One physical source, so an unqualified column can only be that table's. Crediting a
            # column the table does not have would be over-attribution, which only costs a rebuild.
            used[_key(next(iter(physical.values())))].add(name)
        elif len(physical) > 1:
            return (
                f"unqualified column {name!r} with several joined sources "
                f"({sorted(_key(node) for node in physical.values())}); no schema to disambiguate"
            )
    for name in _using_columns(scope, exp):
        # USING names the join column of every source in the join, so credit all of them: too many
        # credits cost a rebuild, missing one would skip work.
        for node in physical.values():
            used[_key(node)].add(name)
    return ""


def _resolve(qualifier: str, database: str, physical: dict, qualifier_index: dict, aliases: dict):
    if database:
        node = _unique(qualifier_index.get(f"{database}.{qualifier}"))
        if node is not None:
            return node
    node = _unique(qualifier_index.get(qualifier))
    if node is not None:
        return node
    if _clean(qualifier) in {_clean(alias) for alias in physical}:
        return None  # a CTE or subquery alias, not a table
    # A correlated reference to a table from an enclosing scope, e.g. EXISTS (... WHERE c.k = o.k).
    return _unique(aliases.get(qualifier))

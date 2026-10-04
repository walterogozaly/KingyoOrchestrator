"""sqlglot-backed column usage, behind the optional `lineage` extra.

Nothing here executes SQL or opens a client: `column_usage` parses a query string
and reports which source columns it reads. `sqlglot` is imported lazily so the rest
of the package works without it, and a missing dependency is reported as a clear
error rather than an `ImportError` from deep inside a walk.

The analysis is deliberately one-sided. Anything that cannot be resolved with
certainty becomes `ColumnUsage.all_used(reason)`, because a false `relevant` only
costs a rebuild while a false `no_op` silently skips work.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from ..core.column_usage import ActionLike, ColumnUsage

_MISSING = (
    "sqlglot is required for column usage analysis; "
    "install the optional extra: pip install 'kingyo-orchestrator[lineage]'"
)


class LineageDependencyError(ImportError):
    """The optional lineage dependency is not installed."""


class ColumnCatalog(Protocol):
    """Optional schema knowledge that makes unqualified columns attributable.

    Without a catalog, an unqualified column is only attributable when exactly one
    source is in scope. With one, a column that a single source declares is
    attributed, and a column that two sources declare is reported as ambiguous.
    """

    def columns(self, table: str) -> tuple[str, ...] | None:
        """Declared column names for a table reference, or None when unknown."""
        ...


class StaticColumnCatalog:
    """An in-memory catalog for callers and tests; unknown tables return None."""

    def __init__(self, tables: Mapping[str, tuple[str, ...]]) -> None:
        self._tables = {key: tuple(value) for key, value in tables.items()}

    def columns(self, table: str) -> tuple[str, ...] | None:
        tail = table.rsplit(".", 1)[-1].casefold()
        for reference, names in self._tables.items():
            if reference.rsplit(".", 1)[-1].casefold() == tail:
                return names
        return None


class _Unresolved(Exception):
    """Raised internally when attribution is not certain; never escapes this module."""


def _part_name(part: Any) -> str:
    """The name behind an identifier-ish AST part."""
    name = getattr(part, "name", None)
    return name if isinstance(name, str) else str(part)


def _sees_aliases(column: Any, select: Any) -> bool:
    """True for a GROUP BY, ORDER BY or HAVING reference, where BigQuery allows an alias."""
    exp = _sqlglot()
    node = column.parent
    while node is not None and node is not select:
        if isinstance(node, (exp.Group, exp.Order, exp.Having)):
            return True
        node = node.parent
    return False


def _declared_by(name: str, sources: Any) -> bool:
    """True when a source in scope is known to declare `name` as a real column."""
    key = name.casefold()
    return any(s.columns is not None and key in s.columns for s in sources)


def _children(node: Any) -> Any:
    """Child AST nodes, flattening list-valued arguments and skipping plain values."""
    for child in node.args.values():
        if hasattr(child, "args"):
            yield child
        elif isinstance(child, (list, tuple)):
            yield from (item for item in child if hasattr(item, "args"))


def _arg(node: Any, *keys: str) -> Any:
    """First present argument among `keys`; sqlglot has renamed argument keys."""
    for key in keys:
        value = node.args.get(key)
        if value is not None:
            return value
    return None


def _from_clause(select: Any, exp: Any) -> Any:
    """The FROM clause, whether sqlglot stores it wrapped in an `exp.From` or bare."""
    clause = _arg(select, "from_", "from")
    if clause is None:
        return None
    return clause if isinstance(clause, exp.From) else _as_from(clause, exp)


def _as_from(expression: Any, exp: Any) -> Any:
    from_clause = exp.From()
    from_clause.set("this", expression)
    return from_clause


_exp: Any = None


def _sqlglot() -> Any:
    global _exp
    if _exp is None:
        try:
            from sqlglot import exp
        except ImportError as error:  # pragma: no cover - depends on the environment
            raise LineageDependencyError(_MISSING) from error
        _exp = exp
    return _exp


class _Source:
    """One table in scope: a physical table, or a derived one (CTE or subquery)."""

    __slots__ = ("alias", "columns", "passthrough", "table")

    def __init__(
        self,
        alias: str,
        table: str | None,
        columns: frozenset[str] | None,
        passthrough: dict[str, frozenset[tuple[str, str]]] | None = None,
    ) -> None:
        self.alias = alias
        self.table = table
        self.columns = columns
        self.passthrough = passthrough or {}

    @property
    def derived(self) -> bool:
        return self.table is None


class _Scope:
    """What one SELECT contributes: the columns it reads and the columns it exposes."""

    __slots__ = ("errors", "exposed", "seen", "used")

    def __init__(self) -> None:
        self.used: dict[str, set[str]] = {}
        self.seen: set[str] = set()
        self.exposed: dict[str, frozenset[tuple[str, str]]] = {}
        self.errors: list[str] = []

    def add(self, table: str, column: str) -> None:
        self.used.setdefault(table, set()).add(column.casefold())


class _Analyzer:
    """Scope-by-scope walk of one query. Instances are single-use per query."""

    def __init__(self, catalog: ColumnCatalog | None) -> None:
        self._exp = _sqlglot()
        self._catalog = catalog
        self._scopes: dict[int, _Scope] = {}

    def analyze(self, tree: Any) -> ColumnUsage:
        exp = self._exp
        selects = list(tree.find_all(exp.Select))
        if not selects:
            return ColumnUsage.all_used("no SELECT statement was found to analyze")
        for select in selects:
            self._analyze_select(select)
        errors = [error for scope in self._scopes.values() for error in scope.errors]
        if errors:
            return ColumnUsage.all_used(errors[0])
        used: dict[str, set[str]] = {}
        for scope in self._scopes.values():
            for table in scope.seen:
                used.setdefault(table, set())
            for table, columns in scope.used.items():
                used.setdefault(table, set()).update(columns)
        return ColumnUsage(
            by_table={table: frozenset(columns) for table, columns in sorted(used.items())}
        )

    # -------------------------------------------------------------- one SELECT
    def _analyze_select(self, select: Any) -> _Scope:
        exp = self._exp
        cached = self._scopes.get(id(select))
        if cached is not None:
            return cached
        scope = _Scope()
        self._scopes[id(select)] = scope

        # Names differ across sqlglot versions, so only check what this one has.
        unsupported = tuple(
            cls
            for name in ("Pivot", "Unpivot", "Lateral")
            if (cls := getattr(exp, name, None)) is not None
        )
        for shape in unsupported:
            if select.find(shape) is not None:
                scope.errors.append(f"{shape.__name__.lower()} is not supported")
                return scope

        sources = self._sources(select)
        if isinstance(sources, str):
            scope.errors.append(sources)
            return scope
        # A table that is read but whose columns are never named still counts as seen.
        scope.seen.update(source.table for source in sources if source.table is not None)

        try:
            # `USING` names arrive as bare identifiers; every other reference is a node.
            columns = [*self._own_columns(select), *self._using_columns(select)]
        except _Unresolved as error:
            scope.errors.append(str(error))
            return scope

        self._record_projections(select, scope, sources)
        if scope.errors:
            return scope

        for column in columns:
            try:
                if isinstance(column, str):
                    pairs = self._attribute_unqualified(column, sources, on_ambiguity="all")
                else:
                    pairs = self._attribute(column, sources, scope.exposed, select)
            except _Unresolved as error:
                scope.errors.append(str(error))
                return scope
            for table, name in pairs:
                scope.add(table, name)
        return scope

    def _sources(self, select: Any) -> list[_Source] | str:
        exp = self._exp
        ctes = self._visible_ctes(select)
        sources: list[_Source] = []
        expressions = []
        from_clause = _from_clause(select, exp)
        if from_clause is not None and from_clause.this is not None:
            expressions.append(from_clause.this)
        for join in select.args.get("joins") or []:
            expressions.append(join.this)
        for expression in expressions:
            source = self._source(expression, ctes)
            if isinstance(source, str):
                return source
            sources.append(source)
        if not sources:
            return "the statement reads no table"
        return sources

    def _visible_ctes(self, select: Any) -> dict[str, Any]:
        """CTEs visible from `select`, including those declared by enclosing queries."""
        ctes: dict[str, Any] = {}
        node = select
        while node is not None:
            with_clause = _arg(node, "with_", "with")
            if with_clause is not None:
                for cte in with_clause.expressions:
                    ctes[cte.alias_or_name.casefold()] = cte.this
            node = node.parent
        return ctes

    def _source(self, expression: Any, ctes: Mapping[str, Any]) -> _Source | str:
        exp = self._exp
        if isinstance(expression, exp.Table):
            name = expression.name
            if not expression.db and not expression.catalog and name.casefold() in ctes:
                inner = self._derived(ctes[name.casefold()])
                if isinstance(inner, str):
                    return inner
                return _Source(name, None, frozenset(inner.exposed), inner.exposed)
            reference = ".".join(
                part for part in (expression.catalog, expression.db, expression.name) if part
            )
            columns = self._declared(reference)
            return _Source(expression.alias_or_name, reference, columns)
        if isinstance(expression, exp.Subquery):
            inner = self._derived(expression.this)
            if isinstance(inner, str):
                return inner
            alias = expression.alias_or_name
            if not alias:
                return "a derived table has no alias, so its columns cannot be attributed"
            return _Source(alias, None, frozenset(inner.exposed), inner.exposed)
        if isinstance(expression, exp.Values):
            return "a VALUES clause reads no source table, so nothing is attributable"
        return f"{type(expression).__name__.lower()} in FROM is not supported"

    def _derived(self, inner: Any) -> _Scope | str:
        if isinstance(inner, self._exp.Subquery):
            inner = inner.this
        if not isinstance(inner, self._exp.Select):
            return "a derived source is not a SELECT, so its columns cannot be attributed"
        scope = self._analyze_select(inner)
        return None if scope.errors else scope

    def _declared(self, reference: str) -> frozenset[str] | None:
        if self._catalog is None:
            return None
        names = self._catalog.columns(reference)
        return None if names is None else frozenset(name.casefold() for name in names)

    def _own_columns(self, select: Any) -> list[Any]:
        """Columns this SELECT reads, excluding those belonging to a nested scope."""
        exp = self._exp
        found: list[Any] = []

        def visit(node: Any) -> None:
            if node is not select and isinstance(node, (exp.Select, exp.SetOperation)):
                return
            if isinstance(node, exp.Star):
                # count(*) reads rows, not a column.
                if not isinstance(node.parent, exp.Count):
                    raise _Unresolved("the query selects a star, so its columns are unknown")
                return
            if isinstance(node, exp.Column):
                if node.find(exp.Star) is not None:
                    raise _Unresolved(
                        "the query selects a qualified star, so its columns are unknown"
                    )
                found.append(node)
                return
            for child in _children(node):
                visit(child)

        visit(select)
        return found

    def _using_columns(self, select: Any) -> list[str]:
        """JOIN ... USING (col) lists bare identifiers, one per joined source column."""
        names: list[str] = []
        for join in _arg(select, "joins") or []:
            for identifier in _arg(join, "using") or []:
                names.append(_part_name(identifier))
        return names

    def _attribute(
        self,
        column: Any,
        sources: list[_Source],
        exposed: Mapping[str, frozenset[tuple[str, str]]] | None = None,
        select: Any = None,
    ) -> list[tuple[str, str]]:
        """Map a column reference to `(source table, source column)` pairs."""
        parts = [_part_name(part) for part in column.parts if part is not None]
        if len(parts) == 1:
            if exposed and select is not None and _sees_aliases(column, select):
                aliased = exposed.get(parts[0].casefold())
                if aliased is not None and not _declared_by(parts[0], sources):
                    return sorted(aliased)
            return self._attribute_unqualified(parts[0], sources)
        # `a.col` is alias + column; `t.struct.field` reads the struct column itself.
        alias, name = parts if len(parts) == 2 else (parts[0], parts[1])
        for source in sources:
            if source.alias.casefold() == alias.casefold():
                return self._attribute_qualified(source, name)
        raise _Unresolved(f"alias {alias!r} does not match any table in scope")

    def _attribute_unqualified(
        self,
        name: str,
        sources: list[_Source],
        *,
        on_ambiguity: str = "unresolved",
    ) -> list[tuple[str, str]]:
        """Attribute a bare column name.

        With one source in scope the name can only come from it. With several it
        needs a catalog, unless the column is declared by exactly one of them.
        `on_ambiguity="all"` is for JOIN ... USING, where the name belongs to every
        declaring source, so all of them are reported instead of giving up.
        """
        key = name.casefold()
        if len(sources) == 1:
            return self._attribute_qualified(sources[0], key)
        candidates = [s for s in sources if s.columns is not None and key in s.columns]
        declaring = ", ".join(sorted(s.alias for s in sources if s.columns is None))
        if declaring:
            raise _Unresolved(
                f"column {name!r} is unqualified and {declaring} has unknown columns; "
                "supply a catalog to attribute it"
            )
        if len(candidates) == 1:
            return self._attribute_qualified(candidates[0], key)
        if len(candidates) > 1 and on_ambiguity == "all":
            return sorted({pair for source in candidates for pair in self._pairs(source, key)})
        if len(candidates) > 1:
            raise _Unresolved(
                f"column {name!r} is declared by more than one source in scope "
                f"({', '.join(sorted(s.alias for s in candidates))})"
            )
        raise _Unresolved(f"column {name!r} matches no source in scope")

    def _pairs(self, source: _Source, name: str) -> frozenset[tuple[str, str]]:
        if source.derived:
            return source.passthrough.get(name, frozenset())
        return frozenset({(source.table, name)})

    def _attribute_qualified(self, source: _Source, name: str) -> list[tuple[str, str]]:
        key = name.casefold()
        if source.derived:
            pairs = source.passthrough.get(key)
            if pairs is None:
                raise _Unresolved(
                    f"column {name!r} is not exposed by {source.alias!r}, "
                    "so it cannot be traced back"
                )
            return sorted(pairs)
        if source.table is None:  # pragma: no cover - defensive
            raise _Unresolved("internal: source has no table")
        return [(source.table, key)]

    def _record_projections(self, select: Any, scope: _Scope, sources: list[_Source]) -> None:
        """Remember which source columns each output column of this SELECT exposes."""
        for projection in select.expressions:
            name = projection.alias_or_name
            pairs: set[tuple[str, str]] = set()
            for column in projection.find_all(self._exp.Column):
                try:
                    pairs.update(self._attribute(column, sources))
                except _Unresolved as error:
                    scope.errors.append(str(error))
                    return
            if name:
                scope.exposed[name.casefold()] = frozenset(pairs)


class SqlglotColumnUsageSource:
    """`ColumnUsageSource` backed by sqlglot, for plain BigQuery SQL."""

    def __init__(self, catalog: ColumnCatalog | None = None, *, dialect: str = "bigquery") -> None:
        self._catalog = catalog
        self._dialect = dialect

    def column_usage(self, action: ActionLike) -> ColumnUsage:
        query = action.query
        if query is None or not query.strip():
            return ColumnUsage.all_used(f"{action.id} has no query text to analyze")
        try:
            from sqlglot import parse_one
        except ImportError:  # pragma: no cover - depends on the environment
            raise LineageDependencyError(_MISSING) from None
        try:
            tree = parse_one(query, dialect=self._dialect)
        except Exception as error:  # sqlglot raises several parse error types
            return ColumnUsage.all_used(f"{action.id} could not be parsed: {type(error).__name__}")
        if tree is None:
            return ColumnUsage.all_used(f"{action.id} has no statement to analyze")
        return _Analyzer(self._catalog).analyze(tree)

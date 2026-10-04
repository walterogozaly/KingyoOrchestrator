"""Partition-locality analysis through CTEs and FROM/JOIN subqueries.

`analysis._analyze_partition` only understands a flat `SELECT ... FROM table [JOIN table]` and gives up
("full") on CTEs and subqueries. This module does the same proof on the *scope tree* of the query
(sqlglot scopes), without rewriting the SQL: each nested query is a scope, and
  1. the model's partition expression is traced through projections of nested scopes down to columns of
     the changed parent (must be row-local: no aggregate / window / subquery on the way);
  2. every scope that reads the parent (directly or via a CTE/derived table) must be partition-local:
     no LIMIT/OFFSET, no global aggregate, every GROUP BY / PARTITION BY / DISTINCT keyed by the partition
     expression (or by all its parent columns), parent not on the nullable side of an outer join;
  3. the parent must be read exactly once overall (a CTE that reads it, referenced twice, counts twice) and
     never inside a WHERE/EXISTS subquery or a set operation.
Same soundness rule as analysis.py: restricting the parent to the affected partitions and recomputing equals the
full result restricted to those partitions. When any condition cannot be shown the answer stays `full`.

Integration hook (one line, in analysis.analyze_edge right after `s = _analyze_partition(model, parent)`):
    s = flatten.refine(model, parent, s, columns)
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, ScopeType, traverse_scope

from . import analysis as A


def _srcs(sc: Scope) -> dict:
    """alias -> Table | Scope actually selected from in this scope (`Scope.sources` also lists every visible CTE)."""
    return {a: s for a, (_, s) in sc.selected_sources.items()}


_SETOP = (
    getattr(ScopeType, "SET_OPERATION", None) or ScopeType.UNION
)  # renamed across sqlglot versions


def _full(reason: str) -> A.Strategy:
    return A.Strategy("full", reason)


def refine(model, parent, s: A.Strategy, columns: dict[str, list[str]] | None = None) -> A.Strategy:
    """Scope-based second opinion on the flat analysis `s`. Never raises.

    * `s` is full  -> try to prove partition-locality through CTEs/subqueries (recovers edges).
    * `s` is aligned/data_dependent -> keep it unless the scoped analysis finds it unsound (e.g. the parent is
      also read inside a WHERE subquery, which the flat analysis does not look at); then downgrade to full.
    """
    try:
        r = analyze_partition_scoped(model, parent, columns)
    except Exception as e:  # unknown => keep the sound answer
        if s.kind == "full":
            return A.Strategy("full", f"{s.reason} (scoped analysis failed: {type(e).__name__})")
        return s
    if s.kind == "full" or r.kind == "full":
        return r
    return s


def analyze_partition_scoped(
    model, parent, columns: dict[str, list[str]] | None = None
) -> A.Strategy:
    if not model.partition_expr:
        return _full("target is unpartitioned")
    ps = A._short(parent.name)
    schema = A._nested_schema(columns) if columns else None
    tree = A.parse_one(model.sql)
    if not isinstance(tree, exp.Select):
        return _full("set operation / non-SELECT body")
    tree = qualify(
        tree,
        schema=schema,
        dialect=A.DIALECT,
        quote_identifiers=False,
        identify=False,
        validate_qualify_columns=False,
    )
    scopes = traverse_scope(tree)
    if not scopes:
        return _full("cannot build scopes")
    root = scopes[-1]

    # number of times each scope reads the parent (a CTE used twice counts twice)
    cnt: dict[int, int] = {}
    for sc in scopes:
        if sc.scope_type == _SETOP:
            cnt[id(sc)] = sum(cnt.get(id(u), 0) for u in sc.union_scopes)
            continue
        n = 0
        for src in _srcs(sc).values():
            n += (src.name == ps) if isinstance(src, exp.Table) else cnt.get(id(src), 0)
        cnt[id(sc)] = n
    reading = [sc for sc in scopes if cnt[id(sc)]]
    if cnt[id(root)] == 0:
        return _full("parent not read by the query")
    if cnt[id(root)] != 1:
        return _full("parent appears more than once (self-join)")
    for sc in reading:
        if sc.scope_type == _SETOP or not isinstance(sc.expression, exp.Select):
            return _full("set operation / non-SELECT body")
        if sc.scope_type in (ScopeType.SUBQUERY, ScopeType.UDTF):
            return _full("parent read inside a subquery expression")

    def reads(src) -> bool:
        return (isinstance(src, exp.Table) and src.name == ps) or (
            isinstance(src, Scope) and cnt.get(id(src), 0) > 0
        )

    why: list[str] = []  # why provenance failed, for reporting

    # ---- provenance: express an expression over the parent's own columns -----------------------------
    def col(c: exp.Column, sc: Scope):
        if c.table:
            src = _srcs(sc).get(c.table)
        else:
            src = next(iter(_srcs(sc).values())) if len(_srcs(sc)) == 1 else None
        if isinstance(src, exp.Table):
            if src.name == ps:
                return exp.column(c.name)
            why.append("partition column comes from a table other than the changed parent")
            return None
        if not isinstance(src, Scope) or not isinstance(src.expression, exp.Select):
            return None
        sel = src.expression
        for p in sel.selects:
            if not isinstance(p, exp.Star) and p.alias_or_name.lower() == c.name.lower():
                return resolve(p.unalias(), src)
        stars = [
            p
            for p in sel.selects
            if isinstance(p, exp.Star)
            or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star))
        ]
        if (
            len(stars) == 1 and len(_srcs(src)) == 1
        ):  # SELECT * over one source: passes the column through
            only = next(iter(_srcs(src)))
            return resolve(exp.column(c.name, table=only), src)
        return None

    def resolve(e: exp.Expression, sc: Scope):
        if e.find(exp.AggFunc, exp.Window):
            why.append("computed by aggregate/window (needs key-based recompute)")
            return None
        if e.find(exp.Subquery, exp.Select):
            why.append("computed by a subquery expression")
            return None
        e = e.copy()
        if isinstance(e, exp.Column):
            return col(e, sc)
        for c in list(e.find_all(exp.Column)):
            r = col(c, sc)
            if r is None:
                return None
            c.replace(r)
        return e

    pexpr = A.parse_one(model.partition_expr)
    top_proj = {
        p.alias_or_name.lower(): p.unalias()
        for p in root.expression.selects
        if not isinstance(p, exp.Star)
    }
    for c in list(pexpr.find_all(exp.Column)):
        inner = top_proj.get(c.name.lower())
        r = resolve(inner, root) if inner is not None else col(exp.column(c.name), root)
        if r is None:
            return _full(
                f"cannot resolve partition column {c.name}" + (f": {why[0]}" if why else "")
            )
        if c.parent is None:  # partition expr is a bare column
            pexpr = r
        else:
            c.replace(r)
    over_parent = pexpr
    pcols = {c.name.lower() for c in over_parent.find_all(exp.Column)}
    if not pcols:
        return _full("partition expr does not depend on the parent")
    pnorm = A._norm(over_parent)

    def keyed(keys, sc) -> bool:
        have_cols, have_expr = set(), False
        for k in keys:
            r = resolve(k, sc)
            if r is None:
                continue
            have_expr |= A._norm(r) == pnorm
            if isinstance(r, exp.Column):
                have_cols.add(r.name.lower())
        return have_expr or pcols <= have_cols

    # ---- every parent-reading scope must be partition-local ------------------------------------------
    for sc in reading:
        e = sc.expression
        if e.args.get("limit") or e.args.get("offset"):
            return _full("LIMIT/OFFSET/QUALIFY")
        own = lambda n, e=e: n.find_ancestor(exp.Select) is e  # noqa: E731
        group = e.args.get("group")
        if group:
            if not keyed(group.expressions, sc):
                return _full("GROUP BY does not include the partition expression")
        elif any(own(a) for a in e.find_all(exp.AggFunc) if not a.find_ancestor(exp.Window)):
            return _full("global aggregate")
        for w in (w for w in e.find_all(exp.Window) if own(w)):
            if not keyed(w.args.get("partition_by") or [], sc):
                return _full("window function not partitioned by the partition expression")
        if e.args.get("distinct") and not keyed(
            [p.unalias() for p in e.selects if not isinstance(p, exp.Star)], sc
        ):
            return _full("SELECT DISTINCT does not include the partition expression")
        # parent-derived sources must not be null-extended by an outer join
        srcs = [e.args["from_"].this] if e.args.get("from_") else []
        for j in e.args.get("joins", []):
            side = (j.side or "").upper()
            nulled = ([j.this] if side in ("LEFT", "FULL") else []) + (
                srcs if side in ("RIGHT", "FULL") else []
            )
            if any(reads(_srcs(sc).get(n.alias_or_name)) for n in nulled):
                return _full("parent is on nullable side of an outer join")
            srcs.append(j.this)

    # mutable sources: name of the output column carrying the parent's unique key, when it is plain row-level
    key_out = None
    if parent.unique_key and all(
        not (
            sc.expression.args.get("group")
            or sc.expression.args.get("distinct")
            or any(sc.expression.find_all(exp.Window))
        )
        for sc in reading
    ):
        for p in root.expression.selects:
            if isinstance(p, exp.Star):
                continue
            r = resolve(p.unalias(), root)
            if isinstance(r, exp.Column) and r.name.lower() == parent.unique_key.lower():
                key_out = p.alias_or_name
                break
    pe_parent = A.parse_one(parent.partition_expr) if parent.partition_expr else None
    aligned = pe_parent is not None and pnorm == A._norm(pe_parent)
    alias = next((a for a, s in _srcs(root).items() if reads(s)), None)
    return A.Strategy(
        "aligned" if aligned else "data_dependent",
        parent_expr=over_parent,
        alias=alias,
        key_out=key_out,
    )


def install() -> None:
    """Make analysis.analyze_edge use the scoped analysis (idempotent). Stand-in for the one-line hook above."""
    orig = A._analyze_partition
    if getattr(orig, "_flatten", False):
        return

    def wrapped(model, parent):
        return refine(model, parent, orig(model, parent))

    wrapped._flatten = True
    A._analyze_partition = wrapped

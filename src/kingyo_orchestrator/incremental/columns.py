"""Column-aware change relevance.

reads(model, parent, columns)            -> parent columns the model's SQL reads at all (None = unknown / all)
changed_outputs(model, parent, changed, columns)
                                         -> the model's output columns that can change when only `changed`
                                            parent columns changed (None = all of them)

Rules (conservative):
  * a changed column used anywhere other than a SELECT expression (WHERE, JOIN ON, GROUP BY, HAVING, window
    PARTITION/ORDER, QUALIFY, ORDER BY, DISTINCT) can add or remove rows -> every output column may change (None);
  * otherwise an output column changes iff its lineage reaches a changed parent column;
  * anything that cannot be resolved (unknown schema, SELECT * without schema, parse errors) -> None.
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope

from . import analysis as A


def _qualified(model, columns):
    return qualify(
        A.parse_one(model.sql),
        schema=A._nested_schema(columns),
        dialect=A.DIALECT,
        quote_identifiers=False,
        identify=False,
    )


_STRUCTURAL = (
    exp.Where,
    exp.Having,
    exp.Qualify,
    exp.Group,
    exp.Order,
    exp.Window,
    exp.Join,
    exp.Limit,
)


def _structural(col, select):
    """True if `col` sits anywhere other than a plain SELECT expression of its own query."""
    node = col
    while node is not None and node is not select:
        if isinstance(node, _STRUCTURAL):
            return True
        node = node.parent
    return bool(select.args.get("distinct"))


def _taint(model, parent, changed, columns):
    """Bottom-up over scopes: which output columns of each scope can change. None = all of them."""
    ps = A._short(parent.name)
    changed = set(changed)
    tree = _qualified(model, columns)
    taint = {}  # id(scope) -> set of output names, or None (all)
    read = set()
    by_expr = {}
    for sc in traverse_scope(tree):
        e = sc.expression
        if isinstance(e, exp.SetOperation):
            # branches were visited just before; positional outputs, so any change in a branch -> all
            branches = [taint.get(by_expr.get(id(x)), None) for x in (e.left, e.right)]
            taint[id(sc)] = set() if all(b == set() for b in branches) else None
            by_expr[id(e)] = id(sc)
            continue
        by_expr[id(e)] = id(sc)
        if not isinstance(e, exp.Select):
            return tree, read, None
        derived = [v for v in sc.sources.values() if not isinstance(v, exp.Table)]
        if any(taint.get(id(v), None) is None for v in derived):
            taint[id(sc)] = None  # rows of a derived source may appear / disappear
            continue
        if any(taint.get(id(q), None) != set() for q in sc.subquery_scopes):
            taint[id(sc)] = None  # a changed value feeds an IN/EXISTS/scalar subquery
            continue

        def hit(c):
            src = sc.sources.get(c.table)  # noqa: B023 (called within this iteration)
            if isinstance(src, exp.Table):
                if src.name == ps:
                    read.add(c.name)
                    return c.name in changed
                return False
            if src is not None and hasattr(src, "expression"):  # derived table / CTE scope
                return c.name in taint[id(src)]
            raise ValueError(f"unresolved column {c.sql()}")

        hits = [c for c in sc.columns if hit(c)]
        if any(_structural(c, e) for c in hits):
            taint[id(sc)] = None
            continue
        out = set()
        for proj in e.selects:
            if isinstance(proj, exp.Star) or proj.find(exp.Star) and not proj.find(exp.Count):
                return tree, read, None
            if any(c in hits for c in proj.find_all(exp.Column)):
                out.add(proj.alias_or_name)
        taint[id(sc)] = out
    return tree, read, taint.get(id(sc), None)


def reads(model, parent, columns):
    """Parent columns the model reads at all (None = unknown)."""
    try:
        _, read, _ = _taint(model, parent, set(), columns)
        return read
    except Exception:
        return None


def changed_outputs(model, parent, changed, columns):
    if changed is None:
        return None
    try:
        return _taint(model, parent, changed, columns)[2]
    except Exception:
        return None

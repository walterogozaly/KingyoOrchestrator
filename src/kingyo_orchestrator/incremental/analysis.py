"""Static analysis: how does model B's output partition relate to changed partitions of parent A?

Outcomes per (parent -> model) edge:
  aligned         B's partition expr, rewritten over A's columns, equals A's partition expr.
                  Affected B partitions == changed A partitions. No data lookup needed.
  data_dependent  B's partition is a function of A's columns but not A's partition expr
                  (e.g. B by order_date, A by last_upd_ts). Affected B partitions are looked up
                  from the changed A rows.
  full            Anything we cannot prove partition-local: recompute the whole model.

Soundness rule for aligned / data_dependent: recomputing only the affected output partitions,
with `parent WHERE <B partition expr over parent cols> IN affected` pushed into the parent,
equals the full result restricted to those partitions. That requires no cross-partition
operation (GROUP BY / window not keyed by the partition expr, LIMIT, subquery/CTE/set-op
shapes we have not modelled, parent on the nullable side of an outer join).
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp
from sqlglot import parse_one as _parse_one

DIALECT = None  # set to "bigquery" for Dataform/BigQuery SQL


def parse_one(sql):
    return _parse_one(sql, read=DIALECT)


@dataclass
class Strategy:
    kind: str  # aligned | data_dependent | full
    reason: str = ""
    parent_expr: exp.Expression | None = None  # B's partition expr over the parent's columns
    alias: str | None = None  # alias the parent has inside B's SQL
    key_out: str | None = None  # output column carrying the parent's unique key, if passed through
    key_col: str | None = None  # keyed strategy: the parent column all aggregation is keyed by
    plan: object = None  # outer_join strategy: kingyo.outer_join.Plan


def _norm(e: exp.Expression) -> str:
    e = e.copy()

    def f(n):
        if isinstance(n, exp.Column):
            return exp.column(n.name.lower())
        if isinstance(n, exp.Date) and isinstance(n.this, exp.Date):
            return n.this
        return n

    return e.transform(f).sql().lower()


def _short(name: str) -> str:
    return name.split(".")[-1]


def _full(reason: str) -> Strategy:
    return Strategy("full", reason)


def analyze_edge(model, parent, columns: dict[str, list[str]] | None = None) -> Strategy:
    """`model`, `parent`: kingyo.sqlx.Model. `columns`: table name -> column names (enables CTE/key analysis)."""
    from . import (
        flatten,
    )  # scope-based proof through CTEs / FROM-JOIN subqueries (also vetoes unsound flat answers)

    s = flatten.refine(model, parent, _analyze_partition(model, parent), columns)
    if s.kind == "full" and model.partition_expr and columns:
        try:
            k = _analyze_keyed(model, parent, columns)
        except Exception as e:  # analysis must never crash a run; unknown => full
            k = None
            s.reason += f" (keyed analysis failed: {type(e).__name__})"
        if k:
            return k
    if s.kind == "full" and model.partition_expr:
        from . import (
            outer_join,
        )  # parent on the nullable side of a LEFT/RIGHT join: recompute by join key

        outer_join.DIALECT = DIALECT
        plan = outer_join.analyze(model.sql, model.partition_expr, parent.name)
        if (
            isinstance(plan, outer_join.Plan) and len(plan.b_cols) == 1
        ):  # pre-image capture is single-column
            return Strategy("outer_join", plan=plan)
    return s


def _analyze_partition(model, parent) -> Strategy:
    if not model.partition_expr:
        return _full("target is unpartitioned")
    tree = parse_one(model.sql)
    if not isinstance(tree, exp.Select):
        return _full("set operation / non-SELECT body")
    if tree.args.get("with_") or tree.args.get("with"):
        return _full("CTEs not modelled yet")
    if tree.args.get("limit") or tree.args.get("offset") or tree.args.get("qualify"):
        return _full("LIMIT/OFFSET/QUALIFY")

    # sources: top-level tables only
    sources = {}  # alias -> table name
    nullable = set()
    from_ = tree.args["from_"] if "from_" in tree.args else tree.args.get("from")
    srcs = [(from_.this, None)] + [
        (j.this, (j.side or "").upper()) for j in tree.args.get("joins", [])
    ]
    for t, _side in srcs:
        if not isinstance(t, exp.Table):
            return _full("subquery in FROM/JOIN")
        sources[t.alias_or_name] = t.name
    for i, (t, side) in enumerate(srcs):
        if side in ("LEFT", "FULL"):
            nullable.add(t.alias_or_name)
        if side in ("RIGHT", "FULL"):
            nullable.update(a for a, _ in [(s[0].alias_or_name, 0) for s in srcs[:i]])
    parent_aliases = [a for a, n in sources.items() if n == _short(parent.name)]
    if len(parent_aliases) != 1:
        return _full("parent appears more than once (self-join)")
    alias = parent_aliases[0]
    if alias in nullable:
        return _full("parent is on nullable side of an outer join")

    # B's partition expr is over B's output columns; express it over the parent's columns.
    pexpr = parse_one(model.partition_expr)
    out_cols = {c.name for c in pexpr.find_all(exp.Column)}
    proj = {}
    star = False
    for p in tree.expressions:
        if isinstance(p, exp.Star):
            star = True
        else:
            proj[p.alias_or_name] = p.this if isinstance(p, exp.Alias) else p
    mapping = {}
    for c in out_cols:
        if c in proj:
            mapping[c] = proj[c].copy()
        elif star and len(sources) == 1:
            mapping[c] = exp.column(c)
        else:
            return _full(f"cannot resolve partition column {c}")

    def sub(n):
        return mapping[n.name].copy() if isinstance(n, exp.Column) and n.name in mapping else n

    over_src = pexpr.transform(sub)
    for c in over_src.find_all(exp.Column):
        owner = (
            sources.get(c.table)
            if c.table
            else (next(iter(sources.values())) if len(sources) == 1 else None)
        )
        if owner != _short(parent.name):
            return _full("partition expr depends on columns outside the changed parent")
    over_parent = over_src.transform(
        lambda n: exp.column(n.name) if isinstance(n, exp.Column) else n
    )

    # cross-partition operations must be keyed by the partition expr
    keys_ok = {_norm(over_parent)} | {c.name.lower() for c in over_parent.find_all(exp.Column)}

    def keyed(exprs) -> bool:
        for k in exprs:
            if isinstance(k, exp.Literal) and k.is_int:
                k = tree.expressions[int(k.name) - 1]
                k = k.this if isinstance(k, exp.Alias) else k
            elif isinstance(k, exp.Column) and k.name in proj and not k.table:
                k = proj[k.name]
            if _norm(k) in keys_ok:
                return True
        return False

    group = tree.args.get("group")
    if group and not keyed(group.expressions):
        return _full("GROUP BY does not include the partition expression")
    for w in tree.find_all(exp.Window):
        if not keyed(w.args.get("partition_by") or []):
            return _full("window function not partitioned by the partition expression")

    key_out = None
    if parent.unique_key and not group:
        for name, e in proj.items():
            if isinstance(e, exp.Column) and e.name == parent.unique_key and e.table in ("", alias):
                key_out = name
        if star and len(sources) == 1:
            key_out = key_out or parent.unique_key

    aligned = parent.partition_expr and _norm(over_parent) == _norm(
        parse_one(parent.partition_expr)
    )
    return Strategy(
        "aligned" if aligned else "data_dependent",
        parent_expr=over_parent,
        alias=alias,
        key_out=key_out,
    )


def pushdown(
    model_sql: str, parent_name: str, parent_expr: exp.Expression, values: list[str]
) -> str:
    """Replace `parent` in the model SQL by a partition-pruned subquery."""
    tree = parse_one(model_sql)
    pred = exp.In(this=parent_expr.copy(), expressions=[exp.Literal.string(v) for v in values])
    for t in list(tree.find_all(exp.Table)):
        if t.name == _short(parent_name):
            alias = t.alias_or_name
            sub = exp.select("*").from_(exp.to_table(parent_name)).where(pred).subquery(alias)
            t.replace(sub)
    return tree.sql(dialect=DIALECT)


def pushdown_where(model_sql: str, parent_name: str, pred_sql: str) -> str:
    """Replace `parent` in the model SQL by `(SELECT * FROM parent WHERE <pred_sql>)`."""
    tree = parse_one(model_sql)
    for t in list(tree.find_all(exp.Table)):
        if t.name == _short(parent_name):
            inner = parse_one(f"SELECT * FROM {parent_name} WHERE {pred_sql}")
            t.replace(inner.subquery(t.alias_or_name))
    return tree.sql(dialect=DIALECT)


def pushdown_keys(model_sql: str, parent_name: str, key_col: str, keys_sql: str) -> str:
    """Replace `parent` by `(SELECT * FROM parent WHERE key IN (<keys_sql>))`: key-pruned, all partitions."""
    tree = parse_one(model_sql)
    for t in list(tree.find_all(exp.Table)):
        if t.name == _short(parent_name):
            alias = t.alias_or_name
            inner = parse_one(f"SELECT * FROM {parent_name} WHERE {key_col} IN ({keys_sql})")
            t.replace(inner.subquery(alias))
    return tree.sql(dialect=DIALECT)


# --------------------------------------------------------------------------------------
# Keyed strategy: every aggregation/window that reads the parent is keyed by one parent
# column K (GROUP BY K, PARTITION BY K, ...) and K is exposed in the output. Then output rows
# for a key set S depend only on parent rows with key in S, so: recompute S from
# `parent WHERE K IN S`, delete S's old rows, insert the new ones. Old and new partitions of
# those keys are discovered from the data, not guessed.
# --------------------------------------------------------------------------------------
def _nested_schema(columns):
    from collections import Counter

    depth = Counter(len(n.split(".")) for n in columns).most_common(1)[0][
        0
    ]  # sqlglot needs uniform depth
    out = {}
    for name, cols in columns.items():
        if len(name.split(".")) != depth or not cols:
            continue
        node = out
        parts = name.split(".")
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node[parts[-1]] = {c: "text" for c in cols}
    return out


def _analyze_keyed(model, parent, columns) -> Strategy | None:
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.optimizer.scope import traverse_scope

    ps = _short(parent.name)
    tree = qualify(
        parse_one(model.sql),
        schema=_nested_schema(columns),
        dialect=DIALECT,
        quote_identifiers=False,
        identify=False,
    )
    scopes = traverse_scope(tree)
    if not scopes:
        return None
    # How many times each scope reads the parent, multiplied through CTE / derived-table references
    # (`selected_sources`: what the scope actually selects from; `sources` also lists every visible CTE).
    # The parent must be read exactly once on the path from the top, and appear exactly once in the
    # whole statement: a second occurrence (a CTE referenced twice, a WHERE/EXISTS subquery over the
    # parent) reads rows outside the key set and breaks key locality.
    nreads: dict[int, int] = {}
    for sc in scopes:  # children before parents
        n = 0
        for _, src in sc.selected_sources.values():
            if isinstance(src, exp.Table):
                n += src.name == ps
            else:
                n += nreads.get(id(src), 0)
        nreads[id(sc)] = n
    reads = {k: v > 0 for k, v in nreads.items()}
    occurrences = sum(1 for t in tree.find_all(exp.Table) if t.name == ps)
    if nreads[id(scopes[-1])] != 1 or occurrences != 1:
        return None

    def trace(col, sc):
        src = sc.sources.get(col.table)
        if isinstance(src, exp.Table):
            return (src.name, col.name)
        if src is None or not isinstance(src.expression, exp.Select):
            return None
        for p in src.expression.selects:
            if p.alias_or_name == col.name:
                u = p.this if isinstance(p, exp.Alias) else p
                return trace(u, src) if isinstance(u, exp.Column) else None
        return None

    def keyset(cols, sc):  # parent columns the given expressions pass through
        out = set()
        for c in cols:
            if isinstance(c, exp.Column):
                t = trace(c, sc)
                if t and t[0] == ps:
                    out.add(t[1])
        return out

    cands = None

    def constrain(keys):
        nonlocal cands
        cands = keys if cands is None else cands & keys

    for sc in scopes:
        if not reads[id(sc)]:
            continue
        e = sc.expression
        if isinstance(e, exp.Union):
            if e.args.get("distinct", True):
                return None  # UNION DISTINCT dedups across branches
            continue
        if not isinstance(e, exp.Select):
            return None
        if e.args.get("limit") or e.args.get("offset"):
            return None
        own = lambda n: n.find_ancestor(exp.Select) is e  # noqa: E731, B023
        if e.args.get("group"):
            constrain(keyset(e.args["group"].expressions, sc))
        elif any(own(a) for a in e.find_all(exp.AggFunc) if not a.find_ancestor(exp.Window)):
            return None  # global aggregate
        for w in (w for w in e.find_all(exp.Window) if own(w)):
            constrain(keyset(w.args.get("partition_by") or [], sc))
        if e.args.get("distinct"):
            constrain(keyset([x.this if isinstance(x, exp.Alias) else x for x in e.selects], sc))
        # nullable side of an outer join: parent-derived sources must not be null-extended
        srcs = [e.args["from_"].this] if e.args.get("from_") else []
        for j in e.args.get("joins", []):
            side = (j.side or "").upper()
            nulled = []
            if side in ("LEFT", "FULL"):
                nulled.append(j.this)
            if side in ("RIGHT", "FULL"):
                nulled.extend(srcs)
            for n in nulled:
                src = sc.sources.get(n.alias_or_name)
                if (isinstance(src, exp.Table) and src.name == ps) or (
                    src is not None and not isinstance(src, exp.Table) and reads.get(id(src))
                ):
                    return None
            srcs.append(j.this)

    top = scopes[-1]
    if not isinstance(top.expression, exp.Select):
        return None
    exposed = {}
    for p in top.expression.selects:
        u = p.this if isinstance(p, exp.Alias) else p
        if isinstance(u, exp.Column):
            t = trace(u, top)
            if t and t[0] == ps:
                exposed.setdefault(t[1], p.alias_or_name)
    keys = set(exposed) if cands is None else cands & set(exposed)
    if not keys:
        return None
    pref = [k for k in sorted(keys) if k == parent.unique_key] or sorted(keys)
    return Strategy("keyed", key_col=pref[0], key_out=exposed[pref[0]])


def row_local(model, parent) -> bool:
    """True if every output row of `model` comes from exactly one row of `parent` (read once, no
    GROUP BY / DISTINCT / window / aggregate / LIMIT / set operation anywhere). Then the output rows
    that can change are exactly those whose parent row changed, which lets a row-level delta (the
    parent's changed unique keys) stand in for whole partitions."""
    try:
        tree = parse_one(model.sql)
    except Exception:
        return False
    ps = _short(parent.name)
    if sum(1 for t in tree.find_all(exp.Table) if t.name == ps) != 1:
        return False
    if tree.find(exp.SetOperation, exp.Window, exp.AggFunc, exp.Limit, exp.Offset, exp.Qualify):
        return False
    for j in tree.find_all(exp.Join):  # null-extended rows have no parent row to key them by
        side = (j.side or "").upper()
        if side in ("RIGHT", "FULL") or (
            side == "LEFT" and isinstance(j.this, exp.Table) and j.this.name == ps
        ):
            return False
        if not isinstance(j.this, exp.Table) and j.this.find(exp.Table) is not None:
            if any(t.name == ps for t in j.this.find_all(exp.Table)) and side == "LEFT":
                return False
    return not any(s.args.get("group") or s.args.get("distinct") for s in tree.find_all(exp.Select))

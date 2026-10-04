"""Incremental refresh when the changed parent is on the NULLABLE side of an outer join.

    child = SELECT ... FROM A LEFT JOIN B ON A.k = B.k [AND ...]       (or the mirrored RIGHT JOIN)

New / changed / deleted rows in B can only change child rows built from A rows whose join key
appears in the changed B rows (old or new version). Partition alignment says nothing here (A's rows
sit in old partitions), so the unit of work is the join key:

  1. K = distinct B join keys of the changed B rows (post-image from the changed partitions, plus
     optional pre-image keys captured by the caller before B was rewritten: deletes, key moves).
  2. key mode       the child exposes the A-side join column(s): DELETE child rows with that key in K,
                    re-run the *unchanged* child SQL with A and B pruned to K, INSERT the result.
     partition mode the child does not expose them, but its partition expr depends on A columns only:
                    P = partitions of A rows with key in K; DELETE P from the child, re-run the SQL
                    with A pruned to P (B whole), INSERT. Costs whole partitions instead of keys.
  3. Early cutoff: hash of deleted rows vs inserted rows; equal => nothing propagates. Otherwise the
     returned partitions (old and new) are what the grandchildren must treat as changed.

Soundness precondition (checked in `analyze`, otherwise `Unsupported(reason)` and the caller keeps its
full-refresh fallback): the child is a plain row-local SELECT over joins (no GROUP BY / window / DISTINCT /
LIMIT / QUALIFY / CTE / set-op), the parent appears once, as the nullable side of exactly one LEFT/RIGHT
join with at least one equi-condition between a column of one preserved base table and a parent column,
and the preserved table is not itself null-extended. Extra ON conjuncts and WHERE predicates on parent
columns (incl. anti-joins `WHERE b.k IS NULL`) are fine: they only restrict which A rows produce output.
NULL keys never match, so they never need recomputation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlglot import exp
from sqlglot import parse_one as _parse_one

DIALECT = None  # set "bigquery" for Dataform SQL


def parse_one(sql):
    return _parse_one(sql, read=DIALECT)


@dataclass
class Unsupported:
    reason: str


@dataclass
class Plan:
    mode: str  # "key" | "partition"
    preserved_alias: str
    preserved_table: str
    parent_alias: str
    a_cols: list[
        str
    ]  # preserved-side join columns used (key mode: only those exposed in the output)
    b_cols: list[str]  # matching parent columns (same order)
    out_cols: list[str] = field(default_factory=list)  # key mode: child output names of a_cols
    a_partition_expr: str | None = (
        None  # partition mode: child partition expr rewritten over A columns
    )
    all_a_cols: list[str] = field(
        default_factory=list
    )  # every equi column (partition mode prunes on these)
    all_b_cols: list[str] = field(default_factory=list)


def _short(n):
    return n.split(".")[-1]


def _conjuncts(e):
    if e is None:
        return []
    if isinstance(e, exp.Paren):
        return _conjuncts(e.this)
    if isinstance(e, exp.And):
        return _conjuncts(e.left) + _conjuncts(e.right)
    return [e]


def analyze(child_sql: str, child_partition_expr: str | None, parent: str) -> Plan | Unsupported:
    try:
        return _analyze(child_sql, child_partition_expr, parent)
    except Exception as e:  # analysis must never crash a run: unknown => caller falls back to full
        return Unsupported(f"analysis failed: {type(e).__name__}: {e}")


def _analyze(sql, part_expr, parent) -> Plan | Unsupported:
    U = Unsupported
    t = parse_one(sql)
    if not isinstance(t, exp.Select):
        return U("set operation / non-SELECT body")
    if t.args.get("with_") or t.args.get("with"):
        return U("CTEs not modelled")
    for k in ("group", "having", "qualify", "limit", "offset", "distinct"):
        if t.args.get(k):
            return U(f"{k.upper()} is not row-local")
    if t.find(exp.Window) or t.find(exp.AggFunc):
        return U("window/aggregate is not row-local")
    ps = _short(parent)
    ptabs = [x for x in t.find_all(exp.Table) if x.name == ps]
    if len(ptabs) != 1:
        return U(f"parent referenced {len(ptabs)} times")
    from_ = t.args.get("from_") or t.args.get("from")
    srcs = [(from_.this, "")] + [(j.this, (j.side or "").upper()) for j in t.args.get("joins", [])]
    joins = t.args.get("joins", [])
    pidx = next((i for i, (s, _) in enumerate(srcs) if s is ptabs[0]), None)
    if pidx is None:
        return U("parent is inside a subquery")
    palias = ptabs[0].alias_or_name
    if any(side == "FULL" for _, side in srcs):
        return U("FULL join")

    # which join null-extends the parent, and which source is preserved
    if pidx > 0 and srcs[pidx][1] == "LEFT":
        j = joins[pidx - 1]
        left = [s for s, _ in srcs[:pidx]]
        if any(side in ("RIGHT",) for _, side in srcs):
            return U("mixed LEFT/RIGHT joins")
        cand = left
    elif pidx == 0 and len(srcs) > 1 and srcs[1][1] == "RIGHT":
        j = joins[0]
        if any(side == "RIGHT" for _, side in srcs[2:]):
            return U("multiple RIGHT joins")
        cand = [srcs[1][0]]
    else:
        return U("parent is not on the nullable side of a LEFT/RIGHT join")

    # equi pairs
    cands = {s.alias_or_name: s for s in cand if isinstance(s, exp.Table)}
    pairs = []  # (preserved alias, a_col, b_col)
    using = j.args.get("using")
    if using:
        if len(cand) != 1 or not isinstance(cand[0], exp.Table):
            return U("USING with several candidate preserved tables")
        for u in using:
            pairs.append((cand[0].alias_or_name, u.name, u.name))
    else:
        for c in _conjuncts(j.args.get("on")):
            if not isinstance(c, exp.EQ):
                continue
            l, r = c.left, c.right  # noqa: E741
            if not (isinstance(l, exp.Column) and isinstance(r, exp.Column)):
                continue
            if r.table == palias and l.table in cands:
                pairs.append((l.table, l.name, r.name))
            elif l.table == palias and r.table in cands:
                pairs.append((r.table, r.name, l.name))
    if not pairs:
        return U("no equi-join condition between a base table and the parent")
    aliases = {p[0] for p in pairs}
    if len(aliases) > 1:
        pairs = [p for p in pairs if p[0] == pairs[0][0]]
    pa = pairs[0][0]
    ptable = cands[pa]
    # the preserved table must not be null-extended itself
    pi = next(i for i, (s, _) in enumerate(srcs) if s is ptable)
    if pi > 0 and pidx != 0 and srcs[pi][1] not in ("", "INNER", "CROSS"):
        return U("preserved table is itself null-extended")
    if any(side == "RIGHT" for _, side in srcs[pi + 1 :]) and pidx != 0:
        return U("preserved table null-extended by a later RIGHT join")

    a_cols, b_cols = [p[1] for p in pairs], [p[2] for p in pairs]
    # output exposure of the preserved join columns
    star_alias = any(
        isinstance(e, exp.Column) and isinstance(e.this, exp.Star) and e.table == pa
        for e in t.expressions
    )
    proj = {}
    for e in t.expressions:
        u = e.this if isinstance(e, exp.Alias) else e
        if isinstance(u, exp.Column) and not isinstance(u.this, exp.Star) and u.table == pa:
            proj.setdefault(u.name, e.alias_or_name)
    exposed = [
        (a, b, proj.get(a, a if star_alias else None)) for a, b in zip(a_cols, b_cols, strict=False)
    ]
    exposed = [x for x in exposed if x[2]]
    if exposed:
        return Plan(
            "key",
            pa,
            ptable.name,
            palias,
            [x[0] for x in exposed],
            [x[1] for x in exposed],
            [x[2] for x in exposed],
            None,
            a_cols,
            b_cols,
        )

    # partition mode: partition expr must be a function of preserved-table columns only
    if not part_expr:
        return U("join key not exposed and child unpartitioned")
    pe = parse_one(part_expr)
    mapping, bad = {}, []
    for c in {x.name for x in pe.find_all(exp.Column)}:
        src = next(
            (
                e.this if isinstance(e, exp.Alias) else e
                for e in t.expressions
                if e.alias_or_name == c
            ),
            None,
        )
        if src is None and star_alias:
            src = exp.column(c, table=pa)
        if src is None:
            bad.append(c)
        else:
            mapping[c] = src.copy()
    if bad:
        return U(f"cannot resolve partition column(s) {bad}")
    over = pe.transform(
        lambda n: mapping[n.name].copy() if isinstance(n, exp.Column) and n.name in mapping else n
    )
    for c in over.find_all(exp.Column):
        if c.table != pa:
            return U("partition expr depends on columns outside the preserved table")
    over = over.transform(lambda n: exp.column(n.name) if isinstance(n, exp.Column) else n)
    return Plan(
        "partition",
        pa,
        ptable.name,
        palias,
        a_cols,
        b_cols,
        [],
        over.sql(dialect=DIALECT),
        a_cols,
        b_cols,
    )


# ---------------------------------------------------------------------------------------------
def parent_keys_sql(plan: Plan, parent: str, parent_partition_expr: str | None, partitions) -> str:
    """SELECT of the (non-null) join keys of the changed parent rows; `partitions=None` => whole parent."""
    cols = ", ".join(plan.b_cols)
    nn = " AND ".join(f"{c} IS NOT NULL" for c in plan.b_cols)
    where = nn
    if partitions is not None and parent_partition_expr:
        lits = ", ".join("'" + str(p).replace("'", "''") + "'" for p in partitions)
        where += f" AND CAST({parent_partition_expr} AS VARCHAR) IN ({lits})"
    return f"SELECT DISTINCT {cols} FROM {parent} WHERE {where}"


def _prune(tree, table_node, cols, keys_table, key_cols):
    alias = table_node.alias_or_name
    bare = table_node.copy()
    bare.set("alias", None)
    lhs = ", ".join(cols)
    rhs = ", ".join(key_cols)
    inner = parse_one(
        f"SELECT * FROM {bare.sql(dialect=DIALECT)} WHERE ({lhs}) IN (SELECT {rhs} FROM {keys_table})"
    )
    table_node.replace(inner.subquery(alias))


def rewrite(
    child_sql: str,
    plan: Plan,
    parent: str,
    keys_table: str,
    parts_table: str | None = None,
    parts_values: list | None = None,
) -> str:
    """Child SQL with the preserved table (and, in key mode, the parent) pruned. Everything else untouched."""
    tree = parse_one(child_sql)
    ps = _short(parent)
    ptab = next(x for x in tree.find_all(exp.Table) if x.name == ps)
    atab = next(
        x
        for x in tree.find_all(exp.Table)
        if x.alias_or_name == plan.preserved_alias and x is not ptab
    )
    if plan.mode == "key":
        kc = [f"k{i}" for i in range(len(plan.a_cols))]
        _prune(tree, atab, plan.a_cols, keys_table, kc)
        ptab = next(x for x in tree.find_all(exp.Table) if x.name == ps)
        _prune(tree, ptab, plan.b_cols, keys_table, kc)
    else:
        alias = atab.alias_or_name
        bare = atab.copy()
        bare.set("alias", None)
        if parts_values is not None:  # statically prunable literal filter (kingyo/predicates.py)
            from .predicates import part_pred

            cond = part_pred(plan.a_partition_expr, parts_values)
        else:
            cond = f"CAST({plan.a_partition_expr} AS VARCHAR) IN (SELECT p FROM {parts_table})"
        inner = parse_one(f"SELECT * FROM {bare.sql(dialect=DIALECT)} WHERE {cond}")
        atab.replace(inner.subquery(alias))
    return tree.sql(dialect=DIALECT)


def refresh(
    con,
    child: str,
    child_sql: str,
    child_partition_expr: str | None,
    plan: Plan,
    parent: str,
    keys_select_sql: str,
) -> dict:
    """Run the incremental refresh on a DuckDB connection (call inside the caller's transaction).

    `keys_select_sql`: SELECT returning one column per `plan.b_cols` (see `parent_keys_sql`; the caller
    may UNION pre-image keys captured before the parent was rewritten).
    Returns {"keys", "deleted", "inserted", "changed_partitions" (set[str], empty if early cutoff hit)}.
    """
    kt, pt = f"_kj_keys_{child}", f"_kj_parts_{child}"
    ncols = len(plan.b_cols)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {kt} AS SELECT * FROM ({keys_select_sql}) q")
    nkeys = con.execute(f"SELECT count(*) FROM {kt}").fetchone()[0]
    out = {"keys": nkeys, "deleted": 0, "inserted": 0, "changed_partitions": set()}
    if not nkeys:
        return out
    cn = [d[0] for d in con.execute(f"SELECT * FROM {kt} LIMIT 0").description]
    kc = [f"k{i}" for i in range(ncols)]
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {kt} AS SELECT "
        + ", ".join(f"{c} AS {k}" for c, k in zip(cn, kc, strict=False))
        + f" FROM {kt}"
    )
    pexpr = child_partition_expr

    def parts_of(where):
        if not pexpr:
            return {"ALL"}
        return {
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT CAST({pexpr} AS VARCHAR) FROM {child} t WHERE {where}"
            ).fetchall()
        }

    if plan.mode == "key":
        where = f"({', '.join(plan.out_cols)}) IN (SELECT {', '.join(kc)} FROM {kt})"
        sql = rewrite(child_sql, plan, parent, kt)
    else:
        akeys = ", ".join(plan.a_cols)
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {pt} AS SELECT DISTINCT CAST({plan.a_partition_expr} AS VARCHAR) AS p "
            f"FROM {plan.preserved_table} WHERE ({akeys}) IN (SELECT {', '.join(kc)} FROM {kt})"
        )
        if not con.execute(f"SELECT count(*) FROM {pt}").fetchone()[0]:
            return out  # no preserved row has a changed key: the child cannot change
        from .predicates import part_pred

        pvals = [r[0] for r in con.execute(f"SELECT p FROM {pt}").fetchall()]
        where = part_pred(pexpr, pvals)
        sql = rewrite(child_sql, plan, parent, kt, pt, parts_values=pvals)
    h = lambda src: con.execute(  # noqa: E731
        f"SELECT count(*), coalesce(sum(hash(t)::HUGEINT),0) FROM {src} t"
    ).fetchone()
    old_parts = parts_of(where)
    old_n, old_h = h(f"(SELECT * FROM {child} WHERE {where})")
    new_tbl = f"_kj_new_{child}"
    if plan.mode == "key":
        con.execute(f"CREATE OR REPLACE TEMP TABLE {new_tbl} AS SELECT * FROM ({sql}) q")
    else:  # whole partitions are rewritten: keep only the output rows that fall in P
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {new_tbl} AS SELECT * FROM ({sql}) t WHERE {where}"
        )
    new_n, new_h = h(new_tbl)
    out["deleted"], out["inserted"] = old_n, new_n
    if (old_n, old_h) != (new_n, new_h):
        if plan.mode == "key" and pexpr and "ALL" not in old_parts:
            from .predicates import part_pred

            con.execute(
                f"DELETE FROM {child} WHERE {part_pred(pexpr, old_parts)} AND {where}"
            )  # prunable
        else:
            con.execute(f"DELETE FROM {child} WHERE {where}")
        con.execute(f"INSERT INTO {child} SELECT * FROM {new_tbl}")
        new_parts = (
            {"ALL"}
            if not pexpr
            else {
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT CAST({pexpr} AS VARCHAR) FROM {new_tbl} t"
                ).fetchall()
            }
        )
        out["changed_partitions"] = old_parts | new_parts
    for t_ in (kt, pt, new_tbl):
        con.execute(f"DROP TABLE IF EXISTS {t_}")
    return out

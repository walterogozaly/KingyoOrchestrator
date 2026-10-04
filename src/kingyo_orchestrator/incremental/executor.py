"""Stateless-process / stateful-store executor: designed to be woken by cron.

All memory lives in the warehouse (`_kingyo_dirty`). A run is
    wake -> read dirty set -> act on what is settled & affordable -> persist -> exit
and is safe to repeat, kill, or overlap-with-nothing (idempotent; resumes where it stopped).

Dirty entries are (model, partition|ALL, since). For a declaration (source) a dirty entry is a
*signal*: "new data in this partition, observed at `since`". For a model it is a *debt*: "this
partition is stale". Processing a model recomputes its dirty partitions, then in ONE transaction
(a) swaps the data, (b) clears its own dirty rows, (c) marks children dirty from the partitions
whose content really changed (early cutoff). A crash anywhere leaves a consistent, resumable state.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import duckdb

from . import outer_join
from .analysis import analyze_edge, pushdown_keys, pushdown_where
from .graph import Dag
from .predicates import part_pred, value_list

INLINE_KEYS = (
    10_000  # up to this many changed keys are inlined as literals (clustering can prune on them)
)

ALL = "ALL"  # sentinel partition: whole table / unknown extent


@dataclass
class Policy:
    settle: timedelta = timedelta(
        minutes=15
    )  # a signal must be this old before we act (data may still be landing)
    full_refresh_ratio: float = 0.25  # if dirty partitions exceed this share of the table, do one full refresh (bench: break-even ~25-30%)
    budget_seconds: float | None = (
        None  # stop starting new models after this long; leftovers wait for next wake
    )
    min_rows_for_cost_rules: int = (
        10_000  # below this, always go incremental (cost rules are noise on tiny tables)
    )
    cutoff_on_full: bool = False
    cutoff_on_partial: bool = False  # big tables: hash rewritten partitions to stop unchanged ones (reads them twice); else propagate all rewritten  # hash before/after a FULL refresh to find changed partitions (2 extra scans); else children get ALL


@dataclass
class Report:
    steps: list[str] = field(default_factory=list)
    propagated: dict[str, object] = field(default_factory=dict)
    deferred: dict[str, str] = field(default_factory=dict)

    def log(self, msg):
        self.steps.append(msg)


def _lit(vs):
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in vs)


class Orchestrator:
    def __init__(self, dag: Dag, con: duckdb.DuckDBPyConnection):
        self.dag, self.con = dag, con
        con.execute("""CREATE TABLE IF NOT EXISTS _kingyo_dirty(
            model VARCHAR, part VARCHAR, since TIMESTAMP, PRIMARY KEY(model, part))""")
        con.execute(
            "ALTER TABLE _kingyo_dirty ADD COLUMN IF NOT EXISTS cols VARCHAR"
        )  # changed columns; NULL = all
        # keys that lived in a parent's partitions BEFORE it was rewritten: a keyed child must also
        # recompute keys whose rows disappeared from those partitions (e.g. an updated row moved away)
        con.execute(
            """CREATE TABLE IF NOT EXISTS _kingyo_oldkeys(child VARCHAR, parent VARCHAR, part VARCHAR, k VARCHAR)"""
        )

    # --- state ---------------------------------------------------------
    def signal(self, table: str, partitions, observed_at: datetime, columns=None):
        """Record 'table got new data in these partitions at observed_at'. Durable, mergeable, idempotent.
        `columns`: the columns that changed, if the caller knows (None = any column, incl. new rows)."""
        self._mark_many(
            table,
            [ALL] if partitions == ALL else [str(p) for p in partitions],
            observed_at,
            columns,
        )

    def _edge(self, child, parent):
        """Edge strategy, cached for the duration of one run (SQL and schemas do not change mid-run)."""
        cache = self.__dict__.setdefault("_edge_cache", {})
        k = (child.name, parent.name)
        if k not in cache:
            cache[k] = analyze_edge(child, parent, self.columns())
        return cache[k]

    def _mark_many(self, model, parts, since, cols=None):
        """cols: changed columns of `model` (None = all). Merging: union, and None (all) wins."""
        parts = list(parts)
        if not parts:
            return
        c = None if cols is None else ",".join(sorted(cols))
        self.con.execute(
            "INSERT INTO _kingyo_dirty (model, part, since, cols) SELECT ?, unnest(?::VARCHAR[]), ?, ? "
            "ON CONFLICT DO UPDATE SET since = least(_kingyo_dirty.since, excluded.since), "
            "cols = CASE WHEN _kingyo_dirty.cols IS NULL OR excluded.cols IS NULL THEN NULL ELSE "
            "array_to_string(list_sort(list_distinct(list_concat(string_split(_kingyo_dirty.cols, ','), "
            "string_split(excluded.cols, ',')))), ',') END",
            [model, parts, since, c],
        )

    def _mark(self, model, part, since, cols=None):
        self._mark_many(model, [part], since, cols)

    def dirty_cols(self, model, parts=None):
        """Changed columns recorded for `model` (None = all / unknown)."""
        q = "SELECT bool_or(cols IS NULL), string_agg(cols, ',') FROM _kingyo_dirty WHERE model=?"
        if parts is not None:
            q += f" AND part IN ({_lit(parts)})"
        any_all, agg = self.con.execute(q, [model]).fetchone()
        if any_all or any_all is None:
            return None
        return {c for c in (agg or "").split(",") if c}

    def dirty(self, model) -> dict[str, datetime]:
        return dict(
            self.con.execute(
                "SELECT part, since FROM _kingyo_dirty WHERE model=?", [model]
            ).fetchall()
        )

    def status(self) -> list[tuple]:
        return self.con.execute(
            "SELECT model, part, since FROM _kingyo_dirty ORDER BY model, part"
        ).fetchall()

    # --- helpers -------------------------------------------------------
    def columns(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for t, c in self.con.execute(
            "SELECT table_name, column_name FROM information_schema.columns ORDER BY ordinal_position"
        ).fetchall():
            out.setdefault(t, []).append(c)
        return out

    def _hashes(self, table, expr, where=""):
        if expr is None:
            return {
                ALL: self.con.execute(
                    f"SELECT coalesce(sum(hash(t)::HUGEINT),0) FROM {table} t"
                ).fetchone()[0]
            }
        q = f"SELECT CAST({expr} AS VARCHAR), sum(hash(t)::HUGEINT) FROM {table} t {where} GROUP BY 1"
        return dict(self.con.execute(q).fetchall())

    def build_all(self):
        for n in self.dag.order:
            m = self.dag.models[n]
            if m.type != "declaration":
                self.con.execute(f"CREATE OR REPLACE TABLE {n} AS {m.sql}")

    def _n_partitions(self, model):
        m = self.dag.models[model]
        if not m.partition_expr or not self._exists(model):
            return 0
        return self.con.execute(
            f"SELECT count(DISTINCT {m.partition_expr}) FROM {model}"
        ).fetchone()[0]

    def _exists(self, t):
        return (
            self.con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name=?", [t]
            ).fetchone()[0]
            > 0
        )

    # --- the wake-up entry point --------------------------------------
    def run_once(self, now: datetime | None = None, policy: Policy | None = None) -> Report:
        now, policy, rep = now or datetime.now(), policy or Policy(), Report()
        self._edge_cache = {}
        t0 = time.monotonic()
        rows = self.con.execute("SELECT count(*) FROM _kingyo_dirty").fetchone()[0]
        if not rows:
            rep.log("nothing dirty: no-op")
            return rep
        for name in self.dag.order:
            dirty = self.dirty(name)
            if not dirty:
                continue
            m = self.dag.models[name]
            if policy.budget_seconds is not None and time.monotonic() - t0 > policy.budget_seconds:
                rep.deferred[name] = "out of time budget"
                rep.log(
                    f"{name}: deferred to next wake (time budget); {len(dirty)} dirty entries kept"
                )
                continue
            if m.type == "declaration":
                young = {p: s for p, s in dirty.items() if s + policy.settle > now}
                ready = {p: s for p, s in dirty.items() if p not in young}
                if young:
                    wake = max(young.values()) + policy.settle
                    rep.deferred[name] = f"settling until {wake:%H:%M}"
                    rep.log(
                        f"{name}: signal(s) for {sorted(young)} too fresh (<{policy.settle}); will act at/after {wake:%H:%M}"
                    )
                if ready:
                    self.con.execute("BEGIN")
                    self._propagate(
                        name,
                        ready,
                        rep,
                        since=min(ready.values()),
                        cols=self.dirty_cols(name, ready),
                    )
                    self.con.execute(
                        "DELETE FROM _kingyo_dirty WHERE model=? AND part IN (" + _lit(ready) + ")",
                        [name],
                    )
                    self.con.execute("COMMIT")
                continue
            # a derived model with debt: skip if any parent is still settling (its debt may grow)
            self._refresh(name, dirty, policy, rep)
        return rep

    # --- propagation: parent partitions changed -> child dirty --------------
    def _propagate(self, parent, parts, rep, since, pre_image=True, cols=None):
        """`pre_image=False`: the parent was rebuilt without capturing the keys of rows that left its
        partitions (too costly on a full rebuild), so key-based children cannot be exact: they get ALL."""
        pm = self.dag.models[parent]
        for name in self.dag.children(parent):
            m = self.dag.models[name]
            s = self._edge(m, pm)
            ccols = None  # the child's own changed columns (None = all)
            if cols is not None:
                ck = ("cols", name, parent, frozenset(cols))
                if ck not in self.__dict__.setdefault("_edge_cache", {}):
                    from . import columns as C

                    self._edge_cache[ck] = C.changed_outputs(m, pm, cols, self.columns())
                ccols = self._edge_cache[ck]
                if ccols is not None and not ccols:
                    rep.log(
                        f"{parent}->{name}: changed columns {sorted(cols)} do not reach {name} -> nothing to do"
                    )
                    continue
            forced = (name, parent) in self.__dict__.get("_force_all", set())
            if forced and s.kind in ("keyed", "outer_join"):
                self.__dict__["_force_all"].discard((name, parent))
                self._mark(name, ALL, since, ccols)
                rep.log(f"{parent}->{name}: too many changed keys -> {name} ALL dirty")
                continue
            if not pre_image and s.kind in ("keyed", "data_dependent", "outer_join"):
                self._mark(name, ALL, since, ccols)
                rep.log(
                    f"{parent}->{name}: {s.kind} edge after a full rebuild of {parent} -> {name} ALL dirty"
                )
                continue
            if s.kind == "outer_join" and ALL not in parts:
                self._mark_many(name, [f"@oj|{parent}|{v}" for v in parts], since, ccols)
                rep.log(
                    f"{parent}->{name} (outer join on {s.plan.b_cols}): join keys of changed {sorted(parts)} dirty"
                )
                continue
            if s.kind == "keyed" and ALL not in parts:
                self._mark_many(name, [f"@key|{parent}|{v}" for v in parts], since, ccols)
                rep.log(
                    f"{parent}->{name} (keyed on {s.key_col}): keys of changed {sorted(parts)} dirty"
                )
                continue
            if ALL in parts or s.kind == "full":
                why = "parent changed ALL" if ALL in parts else f"{s.kind} edge: {s.reason}"
                self._mark(name, ALL, since, ccols)
                rep.log(f"{parent}->{name}: {why} -> {name} ALL dirty")
                continue
            if s.kind == "aligned":
                vals = set(parts)
            else:
                q = (
                    f"SELECT DISTINCT CAST({s.parent_expr.sql()} AS VARCHAR) FROM {parent} "
                    f"WHERE {part_pred(pm.partition_expr, parts)}"
                )
                vals = {r[0] for r in self.con.execute(q).fetchall()}
                # rows that left the changed parent partitions carried old child-partition values
                vals |= {
                    r[0]
                    for r in self.con.execute(
                        f"SELECT k FROM _kingyo_oldkeys WHERE child=? AND parent=? AND part IN ({_lit(parts)})",
                        [name, parent],
                    ).fetchall()
                }
                self.con.execute(
                    "DELETE FROM _kingyo_oldkeys WHERE child=? AND parent=?", [name, parent]
                )
            if pm.unique_key:  # upsert source: superseded row versions live in old partitions
                if not s.key_out:
                    self._mark(name, ALL, since, ccols)
                    rep.log(
                        f"{parent}->{name}: upsert source, key not passed through -> {name} ALL dirty"
                    )
                    continue
                q = (
                    f"SELECT DISTINCT CAST({m.partition_expr} AS VARCHAR) FROM {name} WHERE {s.key_out} IN "
                    f"(SELECT {pm.unique_key} FROM {parent} WHERE {part_pred(pm.partition_expr, parts)})"
                )
                old = {r[0] for r in self.con.execute(q).fetchall()}
                if old - vals:
                    rep.log(
                        f"{parent}->{name}: superseded versions in old partitions {sorted(old - vals)}"
                    )
                vals |= old
            self._mark_many(name, sorted(vals), since, ccols)
            rep.log(f"{parent}->{name} ({s.kind}): dirty {sorted(vals)}")

    # --- refresh one derived model ----------------------------------------
    def _refresh(self, name, dirty, policy, rep):
        m, pe = self.dag.models[name], self.dag.models[name].partition_expr
        keyed = {}  # parent -> parent partitions whose keys must be recomputed
        for d in list(dirty):
            if d.startswith("@key|"):
                _, par, v = d.split("|", 2)
                keyed.setdefault(par, []).append(v)
        ojs = {}  # parent -> parent partitions whose join keys must be recomputed (outer join)
        for d in list(dirty):
            if d.startswith("@oj|"):
                _, par, v = d.split("|", 2)
                ojs.setdefault(par, []).append(v)
        plain = {d: v for d, v in dirty.items() if not d.startswith(("@key|", "@oj|"))}
        full = ALL in dirty or not pe
        self.__dict__["_nk_memo"] = {}
        self.__dict__["_force_all"] = {
            k for k in self.__dict__.get("_force_all", set()) if k[1] != name
        }
        total = self._n_partitions(name)
        big = (
            self.con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            >= policy.min_rows_for_cost_rules
            if pe
            else False
        )
        if big and not full and total and len(plain) > policy.full_refresh_ratio * total:
            full = True
            rep.log(
                f"{name}: {len(dirty)}/{total} partitions dirty > {policy.full_refresh_ratio:.0%} -> one full refresh is cheaper"
            )
        if big and not full and keyed:
            rows = self.con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            for par, pvals in keyed.items():
                nk = self.con.execute(
                    f"SELECT count(*) FROM ({self._keys_sql(name, par, sorted(pvals))[1]})"
                ).fetchone()[0]
                if rows and nk > policy.full_refresh_ratio * rows:
                    full = True
                    rep.log(
                        f"{name}: {nk:,} changed keys vs {rows:,} rows > {policy.full_refresh_ratio:.0%} -> one full refresh is cheaper"
                    )
                    break
        vals = sorted(plain)
        self.con.execute("BEGIN")
        try:
            if full:
                # no pre-image capture: children that need it get ALL below
                hashed = (
                    policy.cutoff_on_full or not pe or not big
                )  # small tables: the exact diff is cheap
                before = self._hashes(name, pe) if hashed else {ALL: 0}
                self.con.execute(f"CREATE OR REPLACE TABLE {name} AS {m.sql}")
                after = self._hashes(name, pe) if hashed else {ALL: 1}
                rep.log(f"{name}: FULL refresh")
            else:
                before, after = {}, {}
                if vals:
                    sql = m.sql
                    for p in m.deps:
                        s = self._edge(m, self.dag.models[p])
                        if s.kind in ("aligned", "data_dependent"):
                            sql = pushdown_where(sql, p, part_pred(s.parent_expr.sql(), vals))
                    where = f"WHERE {part_pred(pe, vals)}"
                    hashed_p = policy.cutoff_on_partial or not big
                    before = self._hashes(name, pe, where) if hashed_p else {v: 0 for v in vals}
                    caps = self._capture(name, where, policy, rep)
                    self.con.execute(f"DELETE FROM {name} {where}")
                    self.con.execute(f"INSERT INTO {name} SELECT * FROM ({sql}) t {where}")
                    self._finish_capture(caps)
                    after = self._hashes(name, pe, where) if hashed_p else {v: 1 for v in vals}
                    rep.log(f"{name}: partitions {vals} recomputed")
                for par, pvals in keyed.items():
                    b, a = self._refresh_keys(
                        name, par, sorted(pvals), rep, hashed=policy.cutoff_on_partial or not big
                    )
                    for k, h in b.items():
                        before.setdefault(k, h)
                    after.update(a)
            extra = set()
            if not full:
                for par, pvals in ojs.items():
                    extra |= self._refresh_outer_join(name, par, sorted(pvals), rep)
            diff = {
                k for k in before.keys() | after.keys() if before.get(k) != after.get(k)
            } | extra
            since = min(dirty.values())
            cols = self.dirty_cols(name, None if full else list(dirty))
            self.con.execute(
                "DELETE FROM _kingyo_dirty WHERE model=?"
                + ("" if full else f" AND part IN ({_lit(dirty)})"),
                [name],
            )
            if diff:
                rep.propagated[name] = diff
                self._propagate(name, diff, rep, since, pre_image=not full, cols=cols)
            else:
                rep.log(f"{name}: output unchanged -> early cutoff")
            if pe:  # drop captured keys that no child still needs
                self.con.execute(
                    "DELETE FROM _kingyo_oldkeys o WHERE parent=? AND NOT EXISTS (SELECT 1 FROM _kingyo_dirty d "
                    "WHERE d.model=o.child AND d.part IN ('@key|' || o.parent || '|' || o.part, '@oj|' || o.parent || '|' || o.part))",
                    [name],
                )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def _capture(self, name, where, policy=None, rep=None):
        """Snapshot, BEFORE `name` is rewritten in `where`, what its key-based children will need to know
        about rows that leave (keys, or child-partition values). Call `_finish_capture` right after the
        rewrite: only values that are no longer present afterwards are persisted (rows that stay are found
        from the current data anyway), which keeps the state table small."""
        pe = self.dag.models[name].partition_expr
        caps = []
        if not pe:
            return caps
        self.con.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _kingyo_cap(child VARCHAR, parent VARCHAR, part VARCHAR, k VARCHAR)"
        )
        for c in self.dag.children(name):
            s = self._edge(self.dag.models[c], self.dag.models[name])
            if s.kind in ("keyed", "data_dependent", "outer_join"):
                what = {"keyed": s.key_col, "outer_join": s.plan and s.plan.b_cols[0]}.get(
                    s.kind
                ) or (s.parent_expr.sql() if s.parent_expr is not None else None)
                if (
                    policy is not None
                    and s.kind in ("keyed", "outer_join")
                    and self._too_many_keys(c, what, name, where, policy, rep)
                ):
                    continue  # the child will be rebuilt in full: no pre-image needed
                sel = f"SELECT DISTINCT '{c}', '{name}', CAST({pe} AS VARCHAR), CAST({what} AS VARCHAR) FROM {name} {where}"
                self.con.execute(f"INSERT INTO _kingyo_cap {sel}")
                caps.append((c, sel))
        return caps

    def _too_many_keys(self, child, key, name, where, policy, rep):
        """Decide up front whether a key-based child would end up rebuilt in full anyway (changed keys
        > full_refresh_ratio of its rows). Scattered updates mark many parent partitions whose keys cover
        most of the child; capturing and counting those keys first costs more than the rebuild saves."""
        if not self.con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name=?", [child]
        ).fetchone()[0]:
            return False
        rows = self.con.execute(f"SELECT count(*) FROM {child}").fetchone()[0]
        if rows < policy.min_rows_for_cost_rules:
            return False
        memo = self.__dict__.setdefault(
            "_nk_memo", {}
        )  # siblings keyed on the same column share one scan
        if (name, key, where) not in memo:
            q = f"SELECT count(DISTINCT {key}) FROM {name} {where}"
            memo[(name, key, where)] = self.con.execute(q).fetchone()[0]
        nk = memo[(name, key, where)]
        if nk <= policy.full_refresh_ratio * rows:
            return False
        self.__dict__.setdefault("_force_all", set()).add((child, name))
        if rep is not None:
            rep.log(
                f"{name}->{child}: {nk:,} keys in the changed partitions vs {child} {rows:,} rows "
                f"> {policy.full_refresh_ratio:.0%} -> {child} will be rebuilt in full; skipping key capture"
            )
        return True

    def _finish_capture(self, caps):
        for c, sel in caps:
            self.con.execute(
                f"INSERT INTO _kingyo_oldkeys SELECT * FROM _kingyo_cap WHERE child='{c}' EXCEPT {sel}"
            )
        if caps:
            self.con.execute("DELETE FROM _kingyo_cap")

    def _refresh_outer_join(self, name, parent, pvals, rep) -> set:
        """Parent is the nullable side of a LEFT/RIGHT join: recompute child rows by join key (kingyo/outer_join.py)."""
        m, pm = self.dag.models[name], self.dag.models[parent]
        s = self._edge(m, pm)
        assert s.kind == "outer_join", f"{name}: edge from {parent} is no longer outer_join"
        plan, b = s.plan, s.plan.b_cols[0]
        typ = self.con.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_name=? AND column_name=?",
            [parent, b],
        ).fetchone()[0]
        keys = (
            outer_join.parent_keys_sql(plan, parent, pm.partition_expr, pvals)
            + f" UNION SELECT CAST(k AS {typ}) FROM _kingyo_oldkeys WHERE child='{name}' AND parent='{parent}' "
            f"AND part IN ({_lit(pvals)}) AND k IS NOT NULL"
        )
        # pre-image of THIS model for its own keyed/data-dependent/outer-join children
        if plan.mode == "key":
            caps = self._capture(name, f"WHERE ({', '.join(plan.out_cols)}) IN ({keys})")
        else:
            parts = [
                r[0]
                for r in self.con.execute(
                    f"SELECT DISTINCT CAST({plan.a_partition_expr} AS VARCHAR) FROM {plan.preserved_table} "
                    f"WHERE ({', '.join(plan.a_cols)}) IN ({keys})"
                ).fetchall()
            ]
            caps = self._capture(name, f"WHERE {part_pred(m.partition_expr, parts)}")
        out = outer_join.refresh(self.con, name, m.sql, m.partition_expr, plan, parent, keys)
        self._finish_capture(caps)
        self.con.execute(
            f"DELETE FROM _kingyo_oldkeys WHERE child='{name}' AND parent='{parent}' AND part IN ({_lit(pvals)})"
        )
        rep.log(
            f"{name}: outer-join recompute ({plan.mode} mode) for {out['keys']} join keys from {parent} {pvals}; "
            f"changed partitions {sorted(out['changed_partitions'])}"
        )
        return out["changed_partitions"]

    def _refresh_keys(self, name, parent, pvals, rep, hashed=True):
        """Recompute only the keys present in the changed parent partitions; replace their old rows."""
        m = self.dag.models[name]
        s, keys_q = self._keys_sql(name, parent, pvals)
        kv = [r[0] for r in self.con.execute(keys_q).fetchall()]
        if len(kv) <= INLINE_KEYS:
            keys = value_list(kv)  # static literal list
        else:
            self.con.execute(f"CREATE OR REPLACE TEMP TABLE _kingyo_keys AS {keys_q}")
            keys = "SELECT * FROM _kingyo_keys"
        new_sql = pushdown_keys(m.sql, parent, s.key_col, keys)
        return self._apply_keys(name, parent, pvals, s, keys, new_sql, rep, hashed)

    def _keys_sql(self, name, parent, pvals):
        m, pm = self.dag.models[name], self.dag.models[parent]
        s = self._edge(m, pm)
        assert s.kind == "keyed", f"{name}: edge from {parent} is no longer keyed"
        typ = self.con.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_name=? AND column_name=?",
            [parent, s.key_col],
        ).fetchone()[0]
        keys = (
            f"SELECT DISTINCT {s.key_col} FROM {parent} "
            f"WHERE {part_pred(pm.partition_expr, pvals)} "
            f"UNION SELECT CAST(k AS {typ}) FROM _kingyo_oldkeys WHERE child='{name}' AND parent='{parent}' AND part IN ({_lit(pvals)})"
        )
        return s, keys

    def _apply_keys(self, name, parent, pvals, s, keys, new_sql, rep, hashed=True):
        pe = self.dag.models[name].partition_expr
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE _kingyo_new AS {new_sql}")
        old = {
            r[0]
            for r in self.con.execute(
                f"SELECT DISTINCT CAST({pe} AS VARCHAR) FROM {name} WHERE {s.key_out} IN ({keys})"
            ).fetchall()
        }
        new = {
            r[0]
            for r in self.con.execute(
                f"SELECT DISTINCT CAST({pe} AS VARCHAR) FROM _kingyo_new"
            ).fetchall()
        }
        touched = sorted(old | new)
        where = f"WHERE {part_pred(pe, touched)}"
        before = self._hashes(name, pe, where) if hashed else {v: 0 for v in touched}
        caps = self._capture(name, where)
        self.con.execute(
            f"DELETE FROM {name} WHERE {part_pred(pe, touched)} AND {s.key_out} IN ({keys})"
        )
        self.con.execute(f"INSERT INTO {name} SELECT * FROM _kingyo_new")
        self._finish_capture(caps)
        after = self._hashes(name, pe, where) if hashed else {v: 1 for v in touched}
        self.con.execute("DROP TABLE _kingyo_new")
        self.con.execute(
            f"DELETE FROM _kingyo_oldkeys WHERE child='{name}' AND parent='{parent}' AND part IN ({_lit(pvals)})"
        )
        rep.log(
            f"{name}: keyed recompute on {s.key_col} from {parent} partitions {pvals}; rows moved between partitions {touched}"
        )
        return before, after

    # --- compat: signal + immediate run -------------------------------
    def on_change(self, table, partitions):
        now = datetime.now()
        self.signal(table, partitions, now - timedelta(days=1))
        return self.run_once(now, Policy(settle=timedelta(0)))


def explain(dag: Dag, columns=None) -> str:
    out = []
    for n in dag.order:
        m = dag.models[n]
        for p in m.deps:
            s = analyze_edge(m, dag.models[p], columns)
            out.append(f"{p:>14} -> {n:<16} {s.kind:<15} {s.reason}")
    return "\n".join(out)

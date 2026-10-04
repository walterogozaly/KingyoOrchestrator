"""Cost KPI for Kingyo: how much BigQuery work does one signal cost, incremental plan vs full rebuild?

KPI (all per signal "table X got new data in `changed` of its N partitions"):

    scan_ratio = bytes scanned by the Kingyo plan (incl. its own overhead)
                 / bytes scanned by re-running every model downstream of X from scratch (the plain-Dataform baseline)
    slot_ratio = same with a slot proxy: scan + write_weight * bytes written

BigQuery on-demand bills *bytes read*; slot time tracks bytes read plus shuffle/write work. This module is a pure
model over a PlanGraph (nodes with sizes, edges with a Kingyo strategy), so the inputs can come from the toy repo,
a synthetic graph with the fixture's strategy mix, the real fixture dump (JSON), or, later, real INFORMATION_SCHEMA
sizes. Every number that is an assumption lives in `Assumptions` and is printed with the result.

Model per node, processed in topological order, with f = fraction of the node's partitions that are dirty:
  aligned         child dirty f_c += f_p;                  parent scan fraction f_p
  data_dependent  child dirty += f_p*late_spread;          parent scan = affected child partitions + changed rows lookup
  keyed           child dirty += f_p*key_span;             parent scan = key_span-wide partitions (old+new versions of keys)
  outer_join      child dirty += f_p*key_span;             parent and the *other* parents scanned over key_span partitions
  full            child dirty = 1;                         every parent scanned in full
Kingyo overhead per refreshed node: before/after per-partition hash (2 x the dirty output partitions read).
Early cutoff (`cutoff`: share of dirty output partitions whose content does not actually change) stops propagation
but not the node's own recompute.
"""

from __future__ import annotations

import json
import random
import statistics
from dataclasses import asdict, dataclass

KINDS = ("aligned", "data_dependent", "keyed", "outer_join", "full")


@dataclass
class Node:
    name: str
    bytes: float  # total size of the table (any unit; ratios only)
    source: bool = False
    partitioned: bool = True
    view: bool = (
        False  # views cost nothing to "refresh" in either plan; dirtiness just passes through
    )


@dataclass
class Edge:
    parent: str
    child: str
    kind: str


@dataclass
class Assumptions:
    changed_fraction: float = (
        1 / 365
    )  # share of the signalled source's partitions that are new/changed (one day of a year)
    late_spread: float = (
        2.0  # data_dependent: child partitions touched per changed parent partition
    )
    key_span: float = 3.0  # keyed/outer_join: partitions (as multiples of changed ones) holding old+new versions of touched keys
    cutoff: float = (
        0.0  # share of dirty partitions that early cutoff proves unchanged (0 = pessimistic)
    )
    write_weight: float = 1.0  # slot proxy: bytes written weigh this much vs bytes read
    hash_overhead: bool = (
        True  # charge Kingyo's before/after partition hash reads (needed for early cutoff)
    )
    min_bill: float = 0.0  # BigQuery bills >= 10 MiB per query, in the same unit as Node.bytes (10 MiB / unit); 0 = ignore
    inc_queries: int = (
        3  # statements per partial refresh (delete/insert/hash lookups), each paying min_bill
    )
    hash_on_full: bool = True  # also hash nodes refreshed in full (today's executor does; skipping saves 2x output reads there)

    def describe(self) -> str:
        return ", ".join(
            f"{k}={v:g}" if not isinstance(v, bool) else f"{k}={v}" for k, v in asdict(self).items()
        )


@dataclass
class PlanGraph:
    nodes: dict[str, Node]
    edges: list[Edge]

    def parents(self, n):
        if getattr(self, "_pm", None) is None or self._pn != len(self.edges):
            self._pm, self._pn = {}, len(self.edges)
            for e in self.edges:
                self._pm.setdefault(e.child, []).append(e)
        return self._pm.get(n, [])

    def order(self) -> list[str]:
        if getattr(self, "_ord", None) is not None and self._on == len(self.edges):
            return self._ord
        from graphlib import TopologicalSorter

        deps = {n: [] for n in self.nodes}
        for e in self.edges:
            deps[e.child].append(e.parent)
        self._ord, self._on = list(TopologicalSorter(deps).static_order()), len(self.edges)
        return self._ord

    def to_json(self) -> str:
        return json.dumps(
            {
                "nodes": [asdict(n) for n in self.nodes.values()],
                "edges": [asdict(e) for e in self.edges],
            }
        )

    @staticmethod
    def from_json(text: str) -> PlanGraph:
        d = json.loads(text)
        return PlanGraph(
            {n["name"]: Node(**n) for n in d["nodes"]}, [Edge(**e) for e in d["edges"]]
        )


@dataclass
class Cost:
    scan: float = 0.0
    write: float = 0.0
    overhead: float = 0.0
    refreshed: int = 0

    def slot(self, a: Assumptions) -> float:
        return self.scan + self.overhead + a.write_weight * self.write

    @property
    def total_scan(self) -> float:
        return self.scan + self.overhead


def cone(g: PlanGraph, source: str) -> set[str]:
    if getattr(g, "_kids", None) is None or g._kn != len(g.edges):
        g._kids, g._kn = {}, len(g.edges)
        for e in g.edges:
            g._kids.setdefault(e.parent, []).append(e.child)
    kids = g._kids
    seen, stack = set(), [source]
    while stack:
        for c in kids.get(stack.pop(), ()):
            if c not in seen:
                seen.add(c)
                stack.append(c)
    return seen


def full_rebuild_cost(g: PlanGraph, source: str, min_bill: float = 0.0) -> Cost:
    c = Cost()
    for n in cone(g, source):
        if g.nodes[n].view:
            continue
        c.scan += max(sum(g.nodes[e.parent].bytes for e in g.parents(n)), min_bill)
        c.write += g.nodes[n].bytes
        c.refreshed += 1
    return c


def incremental_cost(g: PlanGraph, source: str, a: Assumptions) -> Cost:
    c = Cost()
    dirty = {
        source: min(1.0, a.changed_fraction)
    }  # fraction of partitions of a node that are dirty (propagated downstream)
    inc = cone(g, source)
    for n in g.order():
        if n not in inc:
            continue
        node = g.nodes[n]
        d, scan_frac = 0.0, {}
        for e in g.parents(n):
            fp = dirty.get(e.parent, 0.0)
            if fp <= 0:
                continue
            k = e.kind if node.partitioned else "full"
            if k == "aligned":
                d += fp
                scan_frac[e.parent] = scan_frac.get(e.parent, 0) + fp
            elif k == "data_dependent":
                d += fp * a.late_spread
                scan_frac[e.parent] = scan_frac.get(e.parent, 0) + fp * a.late_spread + fp
            elif k == "keyed":
                d += fp * a.key_span
                scan_frac[e.parent] = scan_frac.get(e.parent, 0) + fp * a.key_span + fp
            elif k == "outer_join":
                d += fp * a.key_span
                for o in g.parents(
                    n
                ):  # preserved side(s) are read over the key-affected partitions too
                    scan_frac[o.parent] = scan_frac.get(o.parent, 0) + (
                        fp * a.key_span if o.parent != e.parent else fp * a.key_span + fp
                    )
            else:  # full: recompute the model from scratch
                d = 1.0
                for o in g.parents(n):
                    scan_frac[o.parent] = 1.0
        d = min(1.0, d)
        if node.view:
            dirty[n] = d
            continue
        c.refreshed += 1
        sc = sum(g.nodes[p].bytes * min(1.0, f) for p, f in scan_frac.items())
        c.scan += max(sc, a.min_bill * (1 if d >= 1.0 else a.inc_queries))
        c.write += node.bytes * d
        if a.hash_overhead and (a.hash_on_full or d < 1.0):
            c.overhead += 2 * node.bytes * d
        dirty[n] = d * (1 - a.cutoff)
    return c


@dataclass
class Result:
    source: str
    cone: int
    full: Cost
    inc: Cost
    scan_ratio: float
    slot_ratio: float


def evaluate(g: PlanGraph, source: str, a: Assumptions) -> Result:
    f, i = full_rebuild_cost(g, source, a.min_bill), incremental_cost(g, source, a)
    return Result(
        source,
        f.refreshed,
        f,
        i,
        i.total_scan / f.scan if f.scan else float("nan"),
        i.slot(a) / f.slot(a) if f.slot(a) else float("nan"),
    )


def evaluate_all(g: PlanGraph, a: Assumptions, sources: list[str] | None = None) -> dict:
    """Run every source (or the given ones) as the signal. Weighted by baseline cost = the 'whole workload' ratio."""
    srcs = sources or [n for n, x in g.nodes.items() if x.source and cone(g, n)]
    rs = [evaluate(g, s, a) for s in srcs]
    fs, isc = sum(r.full.scan for r in rs), sum(r.inc.total_scan for r in rs)
    fsl, isl = sum(r.full.slot(a) for r in rs), sum(r.inc.slot(a) for r in rs)
    ratios = sorted(r.scan_ratio for r in rs)
    return {
        "signals": len(rs),
        "scan_ratio_workload": isc / fs,
        "slot_ratio_workload": isl / fsl,
        "scan_ratio_median": statistics.median(ratios),
        "scan_ratio_p90": ratios[int(0.9 * (len(ratios) - 1))],
        "scan_ratio_min": ratios[0],
        "scan_ratio_max": ratios[-1],
        "results": rs,
    }


# --- adapters -----------------------------------------------------------------
def synthetic_graph(
    n_models=3400,
    n_sources=58,
    mix=None,
    layers=10,
    seed=0,
    mean_in_degree=1.9,
    src_bytes=1.0,
    view_share=470 / 3378,
    shrink=0.8,
) -> PlanGraph:
    """Layered random DAG with the fixture's rough size and a given edge-strategy mix. NOT the real topology:
    swap in the real graph (tools_kpi.py json) when it is available. mix = {kind: share}."""
    mix = mix or FIXTURE_MIX
    rnd = random.Random(seed)
    kinds, w = zip(*mix.items(), strict=True)
    nodes = {
        f"s{i}": Node(f"s{i}", src_bytes * rnd.lognormvariate(0, 1), source=True)
        for i in range(n_sources)
    }
    by_layer = {0: list(nodes)}
    edges = []
    per = max(1, (n_models - n_sources) // layers)
    for i in range(n_models - n_sources):
        L = 1 + min(layers - 1, i // per)
        pool = [n for lv in range(max(0, L - 3), L) for n in by_layer.get(lv, [])] or list(nodes)
        k = 1 + (rnd.random() < (mean_in_degree - 1)) + (rnd.random() < max(0, mean_in_degree - 2))
        ps = rnd.sample(pool, min(k, len(pool)))
        name = f"m{i}"
        size = shrink * sum(nodes[p].bytes for p in ps) / len(ps)
        nodes[name] = Node(name, size, view=rnd.random() < view_share)
        by_layer.setdefault(L, []).append(name)
        for p in ps:
            edges.append(Edge(p, name, rnd.choices(kinds, w)[0]))
    return PlanGraph(nodes, edges)


# Strategy mixes over the fixture's 4,254 partitioned-model edges (FIXTURE_FINDINGS.md, tools_flatten_stats.py, outer_join.py).
# Overlap between keyed and flatten-recovered edges is unknown, so these are bounds, not a partition.
def _mix(aligned, keyed, outer, total=4254):
    return {
        "aligned": aligned / total,
        "keyed": keyed / total,
        "outer_join": outer / total,
        "full": (total - aligned - keyed - outer) / total,
    }


MIX_EXECUTOR_NOW = _mix(
    338, 1405, 0
)  # what the executor runs today (outer_join/flatten not wired in): 41% incremental
MIX_CONSERVATIVE = _mix(
    338, 1405, 537
)  # + nullable-side outer-join plans (disjoint from keyed by construction)
MIX_OPTIMISTIC = _mix(
    338 + 701, 1405, 537
)  # + CTE/subquery-flattened aligned edges, assuming no overlap with keyed
FIXTURE_MIX = MIX_CONSERVATIVE


def from_dag(dag, columns, use_outer_join=False, row_counts: dict | None = None) -> PlanGraph:
    """Adapter for the toy/sample repo (kingyo.graph.Dag). Strategies come from the real analyzers (flattening installed)."""
    from . import analysis, flatten

    flatten.install()
    nodes, edges = {}, []
    for n in dag.order:
        m = dag.models[n]
        nodes[n] = Node(
            n,
            float((row_counts or {}).get(n, 1.0)),
            source=m.type == "declaration",
            partitioned=bool(m.partition_expr),
        )
    for n in dag.order:
        m = dag.models[n]
        for p in m.deps:
            s = analysis.analyze_edge(m, dag.models[p], columns)
            kind = s.kind
            if kind == "full" and use_outer_join:
                from . import outer_join

                if not isinstance(
                    outer_join.analyze(m.sql, m.partition_expr, p), outer_join.Unsupported
                ):
                    kind = "outer_join"
            edges.append(Edge(p, n, kind))
    return PlanGraph(nodes, edges)

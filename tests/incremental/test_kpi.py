from kingyo_orchestrator.incremental import kpi


def chain(kinds):
    nodes = {"s": kpi.Node("s", 100, source=True)}
    edges, prev = [], "s"
    for i, k in enumerate(kinds):
        nodes[f"m{i}"] = kpi.Node(f"m{i}", 100)
        edges.append(kpi.Edge(prev, f"m{i}", k))
        prev = f"m{i}"
    return kpi.PlanGraph(nodes, edges)


def test_all_aligned_scales_with_changed_fraction():
    a = kpi.Assumptions(changed_fraction=0.01, hash_overhead=False)
    r = kpi.evaluate(chain(["aligned"] * 3), "s", a)
    assert abs(r.scan_ratio - 0.01) < 1e-9


def test_one_full_edge_cascades_to_everything_below():
    a = kpi.Assumptions(changed_fraction=0.01, hash_overhead=False)
    r = kpi.evaluate(chain(["aligned", "full", "aligned"]), "s", a)
    assert 0.6 < r.scan_ratio < 0.7  # m0 cheap, m1 and m2 full


def test_all_full_with_hashing_is_worse_than_baseline_without_it_equal():
    g = chain(["full"] * 2)
    assert kpi.evaluate(g, "s", kpi.Assumptions()).scan_ratio > 1
    assert abs(kpi.evaluate(g, "s", kpi.Assumptions(hash_on_full=False)).scan_ratio - 1) < 1e-9


def test_json_roundtrip():
    g = chain(["aligned", "keyed"])
    assert kpi.PlanGraph.from_json(g.to_json()).edges == g.edges

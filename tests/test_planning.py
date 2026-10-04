import socket
from dataclasses import FrozenInstanceError

import pytest

from kingyo_orchestrator.planning import GraphCycleError, GraphValidationError, Plan, plan


class MemoryGraph:
    def __init__(self, parents):
        self.adjacency = parents

    def nodes(self):
        return tuple(self.adjacency)

    def parents(self, node):
        return self.adjacency[node]

    def children(self, node):
        return tuple(child for child, parents in self.adjacency.items() if node in parents)


def actions(result):
    return tuple(step.action for step in result.steps)


def test_chain_reports_path_and_orders_dependencies_first(monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("planning must stay offline")

    monkeypatch.setattr(socket, "socket", deny_network)
    graph = MemoryGraph(
        {"source_orders": (), "view_orders": ("source_orders",), "table_daily": ("view_orders",)}
    )
    result = plan(graph, {"source_orders"})
    assert actions(result) == ("view_orders", "table_daily")
    assert result.unknown_sources == ()
    assert (
        result.steps[1].reason
        == "Changed source source_orders via source_orders -> view_orders -> table_daily"
    )
    assert all(step.triggered_by == ("source_orders",) for step in result.steps)


def test_diamond_is_deduplicated_and_shortest_path_ties_are_alphabetical():
    graph = MemoryGraph(
        {
            "source_orders": (),
            "view_b": ("source_orders",),
            "view_a": ("source_orders",),
            "table_daily": ("view_b", "view_a"),
        }
    )
    result = plan(graph, {"source_orders"})
    assert actions(result) == ("view_a", "view_b", "table_daily")
    assert result.steps[-1].reason.endswith("source_orders -> view_a -> table_daily")


def test_two_sources_share_descendants_once_and_report_all_triggers():
    graph = MemoryGraph(
        {
            "source_b": (),
            "source_a": (),
            "view_orders": ("source_b", "source_a"),
            "table_daily": ("view_orders",),
        }
    )
    result = plan(graph, ["source_b", "source_a", "source_a"])
    assert actions(result) == ("view_orders", "table_daily")
    assert all(step.triggered_by == ("source_a", "source_b") for step in result.steps)
    assert result.steps[-1].reason.endswith("source_a -> view_orders -> table_daily")


def test_reason_prefers_shortest_path_across_sources_before_alphabetical_order():
    graph = MemoryGraph(
        {
            "source_a": (),
            "source_z": (),
            "view_orders": ("source_a",),
            "table_daily": ("view_orders", "source_z"),
        }
    )
    result = plan(graph, {"source_a", "source_z"})
    assert result.steps[-1].reason == "Changed source source_z via source_z -> table_daily"
    assert result.steps[-1].triggered_by == ("source_a", "source_z")


def test_shortcut_is_selected_over_longer_path_within_same_source():
    graph = MemoryGraph(
        {
            "source_orders": (),
            "view_orders": ("source_orders",),
            "table_daily": ("source_orders", "view_orders"),
        }
    )
    result = plan(graph, {"source_orders"})
    assert result.steps[-1].reason.endswith("source_orders -> table_daily")


def test_unrelated_branches_and_leaf_sources_are_not_planned():
    graph = MemoryGraph(
        {
            "source_orders": (),
            "table_orders": ("source_orders",),
            "source_customers": (),
            "table_customers": ("source_customers",),
            "source_leaf": (),
        }
    )
    assert actions(plan(graph, {"source_orders", "source_leaf"})) == ("table_orders",)
    assert plan(graph, {"source_leaf"}) == Plan((), ())
    assert plan(graph, {"table_orders"}) == Plan((), ())
    assert plan(graph, set()) == Plan((), ())


def test_unknown_sources_are_reported_sorted_without_discarding_known_impacts():
    graph = MemoryGraph({"source_orders": (), "table_orders": ("source_orders",)})
    result = plan(
        graph, ["source_unknown_z", "source_orders", "source_unknown_a", "source_unknown_a"]
    )
    assert result.unknown_sources == ("source_unknown_a", "source_unknown_z")
    assert actions(result) == ("table_orders",)
    assert plan(graph, ["source_unknown_a"]) == Plan((), ("source_unknown_a",))
    assert plan(MemoryGraph({}), ["source_orders"]) == Plan((), ("source_orders",))


def test_changed_node_is_only_a_step_when_downstream_of_another_changed_source():
    graph = MemoryGraph(
        {"source_orders": (), "view_orders": ("source_orders",), "table_daily": ("view_orders",)}
    )
    result = plan(graph, {"source_orders", "view_orders"})
    assert actions(result) == ("view_orders", "table_daily")
    assert result.steps[0].triggered_by == ("source_orders",)
    assert result.steps[1].triggered_by == ("source_orders", "view_orders")
    assert result.steps[1].reason.endswith("view_orders -> table_daily")


def test_ready_action_ties_are_alphabetical_and_only_affected_parents_constrain_order():
    graph = MemoryGraph(
        {
            "source_orders": (),
            "source_untouched": (),
            "view_untouched_z": ("source_untouched",),
            "table_a": ("source_orders", "view_untouched_z"),
            "table_z": ("source_orders",),
            "table_b": ("table_a",),
        }
    )
    assert actions(plan(graph, {"source_orders"})) == ("table_a", "table_b", "table_z")


def test_input_order_does_not_change_plan_and_results_are_immutable():
    entries = [
        ("source_orders", ()),
        ("table_z", ("source_orders",)),
        ("table_a", ("source_orders",)),
    ]
    expected = plan(MemoryGraph(dict(entries)), {"source_orders"})
    assert plan(MemoryGraph(dict(reversed(entries))), iter(["source_orders"])) == expected
    with pytest.raises(FrozenInstanceError):
        expected.steps = ()
    with pytest.raises(FrozenInstanceError):
        expected.steps[0].action = "table_other"
    with pytest.raises(TypeError):
        expected.steps[0].triggered_by[0] = "source_other"


@pytest.mark.parametrize("changed", [set(), {"source_orders"}, {"source_unknown"}])
@pytest.mark.parametrize(
    "cyclic", [{"table_a": ("table_a",)}, {"table_a": ("table_b",), "table_b": ("table_a",)}]
)
def test_cycles_are_rejected_even_when_unrelated_to_changes(changed, cyclic):
    graph = MemoryGraph({"source_orders": (), **cyclic})
    with pytest.raises(GraphCycleError, match="graph contains a cycle") as exc:
        plan(graph, changed)
    assert exc.value.unresolved_nodes == tuple(sorted(cyclic))


def test_cycle_error_names_unresolved_nodes_including_blocked_descendants():
    graph = MemoryGraph({"table_a": ("table_b",), "table_b": ("table_a",), "table_c": ("table_b",)})
    with pytest.raises(GraphCycleError) as exc:
        plan(graph, {"table_a"})
    assert exc.value.unresolved_nodes == ("table_a", "table_b", "table_c")


def test_missing_adjacency_nodes_are_rejected_instead_of_guessed():
    with pytest.raises(GraphValidationError, match="references missing nodes: source_missing"):
        plan(MemoryGraph({"table_orders": ("source_missing",)}), {"source_missing"})


def test_inconsistent_parent_child_views_are_rejected():
    class InconsistentGraph(MemoryGraph):
        def children(self, node):
            return ()

    with pytest.raises(GraphValidationError, match="parents/children disagree"):
        plan(
            InconsistentGraph({"source_orders": (), "table_orders": ("source_orders",)}),
            {"source_orders"},
        )


@pytest.mark.parametrize("changed", ["source_orders", [""], [None], [42]])
def test_source_ids_require_an_iterable_of_nonempty_strings(changed):
    with pytest.raises(GraphValidationError):
        plan(MemoryGraph({"source_orders": ()}), changed)


def test_duplicate_node_ids_are_rejected():
    class DuplicateGraph(MemoryGraph):
        def nodes(self):
            return ("source_orders", "source_orders")

    with pytest.raises(GraphValidationError, match="duplicate ids"):
        plan(DuplicateGraph({"source_orders": ()}), {"source_orders"})

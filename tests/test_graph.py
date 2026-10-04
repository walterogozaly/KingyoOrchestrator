import json
import socket
import subprocess
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from kingyo_orchestrator.graph import (
    Action,
    ActionId,
    CompilationError,
    CompiledGraphError,
    DependencyGraph,
    MissingDependency,
    UnknownActionError,
    load_compiled_graph,
)


def id_for(name):
    return ActionId("project_x", "dataset_a", name)


def target(name):
    return {"database": "project_x", "schema": "dataset_a", "name": name}


def table(name, *parents, **values):
    return {
        "target": target(name),
        "type": "table",
        "dependencyTargets": [target(parent) for parent in parents],
        **values,
    }


def compiled(*tables, **values):
    return {"tables": list(tables), **values}


def test_linear_chain_and_query_are_loaded_offline(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError("Graph ingestion must not use a network or invoke Dataform")

    monkeypatch.setattr(socket, "socket", deny)
    monkeypatch.setattr(subprocess, "run", deny)
    graph, report = load_compiled_graph(
        compiled(
            table("table_c", "table_b", type="incremental"),
            table("table_a", query="SELECT 1"),
            table("table_b", "table_a", type="view"),
        )
    )
    assert graph.nodes() == tuple(id_for(name) for name in ("table_a", "table_b", "table_c"))
    assert graph.parents(id_for("table_a")) == ()
    assert graph.parents(id_for("table_b")) == (id_for("table_a"),)
    assert graph.children(id_for("table_a")) == (id_for("table_b"),)
    assert graph.descendants(id_for("table_a")) == (id_for("table_b"), id_for("table_c"))
    assert graph.descendants(id_for("table_c")) == ()
    assert graph.actions[0].query == "SELECT 1"
    assert tuple(action.type for action in graph.actions) == ("table", "view", "incremental")
    assert report.cycles == report.missing_dependencies == report.compilation_errors == ()


def test_hand_written_diamond_fixture_preserves_external_sources():
    text = (Path(__file__).parent / "fixtures" / "compiled_graph.json").read_text(encoding="utf-8")
    graph, report = load_compiled_graph(text)
    source = id_for("table_orders")
    assert graph.external_sources == (source,)
    assert graph.parents(source) == ()
    assert graph.children(source) == (id_for("table_clean"),)
    assert graph.descendants(source) == tuple(
        id_for(name) for name in ("table_clean", "table_summary", "view_a", "view_b")
    )
    assert graph.parents(id_for("table_summary")) == (id_for("view_a"), id_for("view_b"))
    assert report.missing_dependencies == (
        MissingDependency(id_for("table_clean"), source, "absent"),
    )


def test_duplicate_ids_are_reported_and_all_definitions_excluded():
    graph, report = load_compiled_graph(
        compiled(
            table("table_a", query="SELECT 1"),
            table("table_a", query="SELECT 2"),
            table("table_b", "table_a"),
        )
    )
    assert report.duplicate_action_ids == (id_for("table_a"),)
    assert tuple(action.id for action in graph.actions) == (id_for("table_b"),)
    assert graph.external_sources == (id_for("table_a"),)
    assert report.missing_dependencies == (
        MissingDependency(id_for("table_b"), id_for("table_a"), "duplicate"),
    )


def test_graph_errors_skip_partial_actions_and_report_unmatched_targets():
    error = {"actionTarget": target("table_a"), "message": "Synthetic compilation failure"}
    unmatched = {"actionTarget": target("table_missing"), "message": "Synthetic missing action"}
    # A failed compile can leave an entry without its normal type/query fields.
    graph, report = load_compiled_graph(
        {
            "tables": [{"target": target("table_a")}, table("table_b", "table_a")],
            "graphErrors": {"compilationErrors": [unmatched, error, error]},
        }
    )
    assert report.skipped_action_ids == (id_for("table_a"),)
    assert report.compilation_errors == (
        CompilationError(id_for("table_a"), "Synthetic compilation failure"),
        CompilationError(id_for("table_missing"), "Synthetic missing action"),
    )
    assert tuple(action.id for action in graph.actions) == (id_for("table_b"),)
    assert graph.external_sources == (id_for("table_a"),)
    assert report.missing_dependencies[0].reason == "graph_error"


def test_cycles_report_exact_members_not_downstream_nodes_and_traversal_terminates():
    graph, report = load_compiled_graph(
        compiled(
            table("table_a", "table_b"),
            table("table_b", "table_a"),
            table("table_c", "table_b"),
            table("table_d", "table_d"),
            table("table_e"),
        )
    )
    assert report.cycles == ((id_for("table_a"), id_for("table_b")), (id_for("table_d"),))
    assert graph.descendants(id_for("table_a")) == (id_for("table_b"), id_for("table_c"))
    assert graph.descendants(id_for("table_d")) == ()


def test_deep_cycle_does_not_require_recursion():
    names = [f"table_{index:04d}" for index in range(1500)]
    tables = [table(name, names[index - 1]) for index, name in enumerate(names)]
    graph, report = load_compiled_graph(compiled(*tables))
    assert report.cycles == (tuple(id_for(name) for name in names),)
    assert len(graph.descendants(id_for(names[0]))) == 1499


def test_deep_acyclic_chain_is_not_reported_as_cycle():
    names = [f"table_{index:04d}" for index in range(1500)]
    tables = [table(names[0])] + [
        table(name, names[index - 1]) for index, name in enumerate(names[1:], 1)
    ]
    graph, report = load_compiled_graph(compiled(*tables))
    assert report.cycles == ()
    assert len(graph.descendants(id_for(names[0]))) == 1499


def test_ordering_is_independent_of_input_and_duplicate_edges():
    values = [table("table_a"), table("table_b", "table_a", "table_a"), table("table_c", "table_a")]
    first = load_compiled_graph(compiled(*values))
    second = load_compiled_graph(compiled(*reversed(values)))
    assert first == second
    assert first[0].parents(id_for("table_b")) == (id_for("table_a"),)


def test_bom_json_and_dictionary_input_agree():
    data = compiled(table("table_a"))
    assert load_compiled_graph("\ufeff" + json.dumps(data)) == load_compiled_graph(data)
    graph, report = load_compiled_graph({"tables": [], "graphErrors": {}})
    assert graph.nodes() == graph.actions == graph.external_sources == report.cycles == ()


def test_input_mutation_does_not_mutate_loaded_graph():
    data = compiled(table("table_a"), table("table_b", "table_a"))
    graph, _ = load_compiled_graph(data)
    data["tables"][0]["target"]["name"] = "table_changed"
    data["tables"][1]["dependencyTargets"].clear()
    assert graph.parents(id_for("table_b")) == (id_for("table_a"),)
    with pytest.raises(FrozenInstanceError):
        graph.actions = ()
    with pytest.raises(FrozenInstanceError):
        graph.actions[0].query = "SELECT 2"
    with pytest.raises(FrozenInstanceError):
        graph.actions[0].id.name = "table_changed"


@pytest.mark.parametrize("method", ["parents", "children", "descendants"])
def test_unknown_graph_node_is_a_clear_error(method):
    graph, _ = load_compiled_graph(compiled(table("table_a")))
    with pytest.raises(UnknownActionError, match="table_missing"):
        getattr(graph, method)(id_for("table_missing"))


@pytest.mark.parametrize(
    "data, message",
    [
        ("not JSON", "Invalid compiled graph JSON"),
        ("[]", "compiled graph must be an object"),
        (None, "compiled graph must be an object"),
        ({}, "requires tables"),
        ({"tables": {}}, "tables must be an array"),
        ({"tables": [None]}, r"tables\[0\]"),
        ({"tables": [{}]}, r"tables\[0\].target"),
        ({"tables": [{"target": {"schema": "dataset_a", "name": "table_a"}}]}, "database"),
        (compiled(table("table_a", type="operation")), "Action.type"),
        (compiled(table("table_a", query=42)), "Action.query"),
        (compiled(table("table_a", disabled=True)), "disabled"),
        (compiled(table("table_a", disabled="false")), "disabled"),
        (compiled(table("table_a", dependencyTargets=None)), "dependencyTargets"),
        (
            compiled(table("table_a", dependencyTargets=[{"name": "table_missing"}])),
            "dependencyTargets",
        ),
        (compiled(graphErrors=[]), "graphErrors"),
        (compiled(graphErrors={"compilationErrors": {}}), "compilationErrors"),
        (compiled(graphErrors={"actionErrors": []}), "supports only compilationErrors"),
        (
            compiled(graphErrors={"compilationErrors": [{"message": "unscoped"}]}),
            "requires actionTarget",
        ),
        (
            compiled(
                graphErrors={
                    "compilationErrors": [{"actionTarget": target("table_a"), "message": 1}]
                }
            ),
            "message",
        ),
        (compiled(operations=[{"target": target("table_operation")}]), "Nonempty operations"),
        (compiled(declarations=[{"target": target("table_source")}]), "Nonempty declarations"),
    ],
)
def test_malformed_and_unsupported_shapes_are_rejected(data, message):
    with pytest.raises(CompiledGraphError, match=message):
        load_compiled_graph(data)


def test_optional_dependencies_and_ignored_descriptive_metadata():
    data = {
        "tables": [{"target": target("table_a"), "type": "table", "tags": ["synthetic"]}],
        "operations": [],
        "projectConfig": {"warehouse": "bigquery"},
    }
    graph, _ = load_compiled_graph(data)
    assert graph.parents(id_for("table_a")) == ()
    assert graph.actions[0].query is None


def test_direct_types_require_immutable_valid_values():
    with pytest.raises(CompiledGraphError, match="simple identifier"):
        ActionId("project_x", "dataset_a", "")
    with pytest.raises(CompiledGraphError, match="tuple"):
        Action(id_for("table_a"), "table", [])
    with pytest.raises(CompiledGraphError, match="tuple"):
        DependencyGraph([])
    action = Action(id_for("table_a"), "table")
    with pytest.raises(CompiledGraphError, match="unique"):
        DependencyGraph((action, replace(action, query="SELECT 1")))

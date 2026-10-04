"""Per-consumer column relevance (#13): is a changed column used downstream at all?

Offline, synthetic SQL only. The sqlglot-backed source is skipped without the `lineage` extra; the
planner itself is tested against a stub source so it never depends on the parser.
"""

from __future__ import annotations

import sys

import pytest

from kingyo_orchestrator.graph.types import Action, ActionId, DependencyGraph
from kingyo_orchestrator.lineage import (
    ChildRelevance,
    ColumnUsage,
    LineageDependencyError,
    relevance,
)
from kingyo_orchestrator.lineage.types import ColumnUsageSource

pytest.importorskip("sqlglot")

from kingyo_orchestrator.lineage import SqlglotColumnUsage  # noqa: E402

DB = ActionId("example-project", "analytics", "orders")
USAGE = SqlglotColumnUsage()


def action(name: str, query: str | None, *deps: ActionId) -> Action:
    """A child of `orders` by default, so `relevance` has something to walk."""
    return Action(ActionId("example-project", "analytics", name), "table", deps or (DB,), query)


# --------------------------------------------------------------------------- ColumnUsage


def test_column_usage_is_immutable_and_typed():
    usage = ColumnUsage.resolved({"p.d.orders": {"id", "amount"}})
    assert usage.columns_for("p.d.orders") == frozenset({"id", "amount"})
    assert usage.reads("orders") is True
    assert usage.unresolved is False
    with pytest.raises(TypeError):
        usage.used["p.d.orders"] = frozenset({"x"})  # type: ignore[index]


def test_unresolved_usage_needs_a_reason():
    with pytest.raises(ValueError):
        ColumnUsage({}, unresolved=True, reason="")


def test_columns_for_matches_any_spelling_of_the_table():
    usage = ColumnUsage.resolved({"analytics.orders": {"id"}})
    assert usage.columns_for("example-project.analytics.orders", "orders") == frozenset({"id"})
    assert usage.columns_for("customer") == frozenset()


# --------------------------------------------------------------------------- plain SQL


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT o.id, o.amount FROM `example-project.analytics.orders` o", {"id", "amount"}),
        (
            "SELECT id FROM analytics.orders WHERE loaded_at > CURRENT_TIMESTAMP()",
            {"id", "loaded_at"},
        ),
        ("SELECT id FROM orders", {"id"}),
        ("SELECT d.orders.id FROM `example-project.analytics.orders`", {"id"}),
        ("SELECT sum(amount) FROM orders", {"amount"}),
        ("SELECT custkey FROM orders GROUP BY 1 HAVING sum(amount) > 1", {"custkey", "amount"}),
        (
            "SELECT id FROM orders GROUP BY id ORDER BY id DESC LIMIT 10",
            {"id"},
        ),
        (
            "SELECT id FROM orders WHERE custkey IN (SELECT custkey FROM customer)",
            {"id", "custkey"},
        ),
        (
            "SELECT id, row_number() OVER (PARTITION BY custkey ORDER BY ts) rn "
            "FROM orders QUALIFY rn = 1",
            {"id", "custkey", "ts"},
        ),
        (
            "SELECT o.id FROM orders o JOIN customer c ON c.custkey = o.custkey",
            {"id", "custkey"},
        ),
        ("SELECT o.amount FROM orders o JOIN customer USING (custkey)", {"amount", "custkey"}),
    ],
)
def test_plain_sql_reports_the_columns_it_reads(query, expected):
    usage = USAGE.column_usage(action("m", query))
    assert not usage.unresolved, usage.reason
    assert usage.columns_for("orders") == expected


def test_unqualified_columns_follow_the_single_source():
    usage = USAGE.column_usage(action("m", "SELECT id, amount FROM analytics.orders"))
    assert usage.used == {"analytics.orders": frozenset({"id", "amount"})}


def test_aliases_resolve_to_the_table_they_name():
    usage = USAGE.column_usage(
        action("m", "SELECT x.c FROM `p.d.orders` x JOIN `p.d.customer` y ON y.k = x.k")
    )  # noqa: E501
    assert usage.columns_for("p.d.orders") == frozenset({"c", "k"})
    assert usage.columns_for("p.d.customer") == frozenset({"k"})


def test_columns_used_only_in_a_cte_still_count():
    usage = USAGE.column_usage(
        action("m", "WITH s AS (SELECT id, amount FROM orders) SELECT amount FROM s")
    )
    assert usage.columns_for("orders") == frozenset({"id", "amount"})


def test_columns_reached_through_a_subquery_count():
    usage = USAGE.column_usage(
        action(
            "m",
            "SELECT s.amount FROM orders o JOIN (SELECT custkey, amount FROM orders2) s "
            "ON s.custkey = o.custkey",
        )
    )
    assert usage.columns_for("orders2") == frozenset({"amount", "custkey"})
    assert usage.columns_for("orders") == frozenset({"custkey"})


def test_correlated_reference_to_an_outer_table_is_attributed():
    usage = USAGE.column_usage(
        action(
            "m",
            "SELECT o.id FROM orders o WHERE EXISTS "
            "(SELECT 1 FROM customer c WHERE c.custkey = o.custkey)",
        )
    )
    assert usage.columns_for("orders") == frozenset({"id", "custkey"})
    assert usage.columns_for("customer") == frozenset({"custkey"})


# --------------------------------------------------------------------------- conservative fallback


@pytest.mark.parametrize(
    "query, fragment",
    [
        ("SELECT * FROM analytics.orders", "cannot be enumerated"),
        ("SELECT o.* FROM analytics.orders o", "cannot be enumerated"),
        ("SELECT FROM WHERE (((", "not parseable"),
        ("SELECT id FROM ${ref('orders')}", "not plain SQL"),
        (
            "SELECT id FROM analytics.orders o JOIN customer c ON c.k = o.k",
            "several joined sources",
        ),  # noqa: E501
        ("SELECT zz.id FROM analytics.orders o", "unresolvable table alias"),
        ("WITH s AS (SELECT * FROM analytics.orders) SELECT s.id FROM s", "cannot be enumerated"),
    ],
)
def test_unresolvable_usage_is_conservative(query, fragment):
    usage = USAGE.column_usage(action("m", query))
    assert usage.unresolved is True
    assert fragment in usage.reason


def test_missing_or_empty_query_is_unresolved():
    assert USAGE.column_usage(action("m", None)).unresolved is True
    assert USAGE.column_usage(action("m", "   ")).unresolved is True


def test_the_fallback_treats_every_column_as_used():
    usage = ColumnUsage.unresolved_fallback("SELECT * cannot be enumerated")
    assert usage.unresolved is True
    assert usage.used == {}


def test_a_star_over_a_cte_is_fine_when_the_cte_body_is_resolvable():
    usage = USAGE.column_usage(action("m", "WITH s AS (SELECT id FROM orders) SELECT * FROM s"))
    assert usage.unresolved is False
    assert usage.columns_for("orders") == frozenset({"id"})


def test_missing_extra_raises_a_clear_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "sqlglot", None)
    with pytest.raises(LineageDependencyError, match="lineage"):
        SqlglotColumnUsage().column_usage(action("m", "SELECT id FROM orders"))


# --------------------------------------------------------------------------- relevance()


def graph_with(*actions: Action) -> DependencyGraph:
    return DependencyGraph(actions)


def test_relevance_is_keyed_by_child_not_a_global_verdict():
    reads = action("reads_amount", "SELECT id, amount FROM `example-project.analytics.orders`")
    ignores = action("ignores_amount", "SELECT id FROM `example-project.analytics.orders`")
    graph = graph_with(reads, ignores)
    answers = relevance(graph, DB, ["amount"])

    assert set(answers) == {reads.id, ignores.id}
    assert answers[reads.id].relevant and not answers[reads.id].no_op
    assert answers[reads.id].columns == ("amount",)
    assert answers[ignores.id].no_op
    assert "none of ['amount'] changed" in answers[ignores.id].reason


def test_an_audit_column_read_by_nobody_is_a_no_op_for_every_child():
    first = action("a", "SELECT id FROM `example-project.analytics.orders`")
    second = action("b", "SELECT count(*) AS n FROM `example-project.analytics.orders`")
    graph = graph_with(first, second)
    answers = relevance(graph, DB, ["ingested_at"])

    assert all(answer.no_op for answer in answers.values())
    assert all(answer.reason for answer in answers.values())


def test_a_column_used_only_in_a_filter_still_counts():
    filtered = action("f", "SELECT id FROM `example-project.analytics.orders` WHERE loaded_at > x")  # noqa: E501
    joined = action(
        "j",
        "SELECT o.id FROM `example-project.analytics.orders` o "
        "JOIN `example-project.analytics.customer` c ON c.orders_id = o.loaded_at",
    )
    graph = graph_with(filtered, joined)
    answers = relevance(graph, DB, ["loaded_at"])

    assert answers[filtered.id].relevant
    assert answers[joined.id].relevant


def test_relevance_with_changed_keys():
    reader = action("r", "SELECT id, lo_orderkey FROM `example-project.analytics.orders`")
    graph = graph_with(reader)

    by_column = relevance(graph, DB, ["amount"])
    assert by_column[reader.id].no_op

    by_key = relevance(graph, DB, ["amount"], ["lo_orderkey"])
    assert by_key[reader.id].relevant
    assert by_key[reader.id].keys == ("lo_orderkey",)
    assert by_key[reader.id].columns == ()


def test_an_unresolvable_child_is_always_relevant():
    star = action("s", "SELECT * FROM `example-project.analytics.orders`")
    graph = graph_with(star)
    answer = relevance(graph, DB, ["loaded_at"])[star.id]

    assert answer.relevant and answer.unresolved
    assert "cannot be enumerated" in answer.reason


def test_children_of_other_parents_are_not_asked():
    customers = ActionId("example-project", "analytics", "customers")
    orders = Action(DB, "table", (), "SELECT 1 AS x")
    graph = graph_with(orders, action("c", "SELECT id FROM customers", customers))
    assert relevance(graph, DB, ["x"]) == {}


def test_a_leaf_has_no_children_and_an_unknown_node_raises():
    leaf = Action(ActionId("example-project", "analytics", "leaf"), "table", (), "SELECT 1 AS x")
    assert relevance(graph_with(leaf), leaf.id, ["x"]) == {}
    with pytest.raises(KeyError):
        relevance(graph_with(leaf), ActionId("example-project", "analytics", "nope"), ["x"])


# --------------------------------------------------------------------------- swappable source


class StubSource:
    """A `ColumnUsageSource` with no parser, proving the planner does not need sqlglot."""

    def __init__(self, usage: ColumnUsage) -> None:
        self.usage = usage

    def column_usage(self, action) -> ColumnUsage:
        return self.usage


def test_the_lineage_source_is_swappable():
    reader = action("r", "anything at all, not even SQL")
    graph = graph_with(reader)

    resolved = relevance(
        graph, DB, ["amount"], source=StubSource(ColumnUsage.resolved({"orders": {"amount"}}))
    )  # noqa: E501
    assert resolved[reader.id].relevant

    unresolved = relevance(
        graph, DB, ["amount"], source=StubSource(ColumnUsage.unresolved_fallback("nope"))
    )
    assert unresolved[reader.id].relevant and unresolved[reader.id].unresolved

    assert isinstance(StubSource(ColumnUsage.resolved()), ColumnUsageSource)


def test_a_failing_backend_degrades_to_relevant():
    class Broken:
        def column_usage(self, action):
            raise ValueError("backend is down")

    graph = graph_with(action("r", "SELECT id FROM analytics.orders"))
    answer = relevance(graph, DB, ["amount"], source=Broken())[
        ActionId("example-project", "analytics", "r")
    ]
    assert answer.relevant and answer.unresolved
    assert "backend failed" in answer.reason


def test_child_relevance_exposes_both_sides():
    answer = ChildRelevance(DB, relevant=False, reason="nothing read")
    assert answer.no_op is True
    assert ChildRelevance(DB, relevant=True, reason="read").no_op is False

"""Per-consumer column relevance, without the optional lineage dependency.

The sqlglot-backed source is exercised in `test_lineage_sqlglot.py`. These tests
pin the pure decisions: how a usage answer is interpreted, and the rule that an
unresolved consumer is always `relevant`.
"""

from datetime import UTC, datetime

import pytest

from kingyo_orchestrator.core.column_usage import (
    ColumnUsage,
    ConsumerAction,
    ConsumerRelevance,
    relevance,
)

ORDERS = "project_x.dataset_a.table_orders"


class FakeUsageSource:
    """Answers from a fixed table of queries; unresolved where the key is missing."""

    def __init__(self, answers: dict[str, ColumnUsage]) -> None:
        self._answers = answers
        self.asked: list[str] = []

    def column_usage(self, action):
        self.asked.append(action.id)
        return self._answers.get(
            action.id, ColumnUsage.all_used(f"no answer registered for {action.id}")
        )


def graph() -> dict[str, ConsumerAction]:
    return {
        ORDERS: ConsumerAction(id=ORDERS, query="SELECT 1"),
        "dataset_a.table_daily": ConsumerAction(
            id="dataset_a.table_daily",
            query="SELECT order_id FROM orders",
            depends_on=(ORDERS,),
        ),
        "dataset_a.table_audit": ConsumerAction(
            id="dataset_a.table_audit",
            query="SELECT order_id FROM orders",
            depends_on=(ORDERS,),
        ),
    }


def used(columns, table=ORDERS):
    return ColumnUsage(by_table={table: frozenset(columns)})


def test_column_usage_matches_graph_ids_and_sql_references():
    usage = used(["order_id"], table="dataset_a.table_orders")
    assert usage.uses(ORDERS, "order_id") is True
    assert usage.uses(ORDERS, "ORDER_ID") is True, "BigQuery identifiers are case-insensitive"
    assert usage.uses(ORDERS, "amount") is False
    assert usage.uses("dataset_a.table_other", "order_id") is False


def test_unresolved_usage_never_answers_a_column():
    usage = ColumnUsage.all_used("the query selects a star")
    assert usage.unresolved
    assert usage.uses(ORDERS, "anything") is None
    assert usage.matching_columns(ORDERS, ("a", "b")) == ()
    assert usage.reason == "the query selects a star"


def test_relevance_is_keyed_by_child_not_by_one_global_verdict():
    source = FakeUsageSource(
        {
            "dataset_a.table_daily": used(["order_id", "amount"]),
            "dataset_a.table_audit": used(["order_id"]),
        }
    )
    results = relevance(graph(), ORDERS, ("amount",), source=source)

    assert sorted(results) == ["dataset_a.table_audit", "dataset_a.table_daily"]
    assert results["dataset_a.table_daily"].relevant is True
    assert results["dataset_a.table_daily"].columns == ("amount",)
    assert results["dataset_a.table_audit"].verdict == "no_op"
    assert results["dataset_a.table_audit"].columns == ()
    assert "amount" in results["dataset_a.table_audit"].reason


def test_changed_keys_make_a_child_relevant_even_when_no_column_is_read():
    source = FakeUsageSource({"dataset_a.table_daily": used(["order_id"])})
    results = relevance(
        graph(),
        ORDERS,
        ("amount",),
        ("order_id",),
        source=source,
    )

    daily = results["dataset_a.table_daily"]
    assert daily.relevant is True
    assert daily.keys == ("order_id",)
    assert daily.columns == ()
    assert "key order_id" in daily.reason


def test_unresolved_child_is_relevant_and_says_why():
    source = FakeUsageSource({"dataset_a.table_daily": used(["order_id"])})
    results = relevance(graph(), ORDERS, ("amount",), source=source)

    audit = results["dataset_a.table_audit"]
    assert audit.relevant is True, "an unresolved consumer must never be skipped"
    assert audit.reason == (
        "usage unresolved, so every column counts: no answer registered for dataset_a.table_audit"
    )


def test_without_a_source_every_child_is_relevant():
    results = relevance(graph(), ORDERS, ("amount",))
    assert results["dataset_a.table_daily"].relevant is True
    assert all("no column usage source" in r.reason for r in results.values())


def test_only_direct_consumers_are_reported():
    deep = dict(graph())
    deep["dataset_a.table_report"] = ConsumerAction(
        id="dataset_a.table_report", depends_on=("dataset_a.table_daily",)
    )
    source = FakeUsageSource({})
    results = relevance(deep, ORDERS, ("amount",), source=source)

    assert sorted(results) == ["dataset_a.table_audit", "dataset_a.table_daily"]
    assert "dataset_a.table_report" not in results, "one hop only; see docs"


def test_relevance_rejects_a_non_mapping_graph():
    with pytest.raises(TypeError, match="mapping"):
        relevance([ConsumerAction(id="a")], ORDERS, ("amount",))  # type: ignore[arg-type]


def test_consumer_relevance_verdict_is_a_stable_string():
    result = ConsumerRelevance("child", False, (), (), "reads nothing")
    assert result.verdict == "no_op"
    assert ConsumerRelevance("child", True, ("a",), (), "reads a").verdict == "relevant"


def test_fingerprint_interaction_point_uses_audit_columns_as_known_unread():
    """#12 configured audit columns feed in as changed columns nobody reads."""
    fingerprint = datetime(2026, 1, 5, tzinfo=UTC)
    assert fingerprint.tzinfo is not None
    source = FakeUsageSource({"dataset_a.table_daily": used(["order_id"])})
    results = relevance(graph(), ORDERS, ("_fingerprint",), source=source)

    assert results["dataset_a.table_daily"].verdict == "no_op"


def test_missing_optional_dependency_raises_a_clear_error(monkeypatch):
    """Importing the lineage package works without sqlglot; calling it says what to install."""
    import importlib.abc
    import sys

    from kingyo_orchestrator.lineage import sqlglot_source

    class Blocked(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] == "sqlglot":
                raise ImportError("sqlglot is not installed")
            return None

    monkeypatch.setattr(sqlglot_source, "_exp", None)
    monkeypatch.setitem(sys.modules, "sqlglot", None)
    monkeypatch.setattr(sys, "meta_path", [Blocked(), *sys.meta_path])

    source = sqlglot_source.SqlglotColumnUsageSource()
    with pytest.raises(
        sqlglot_source.LineageDependencyError, match=r"kingyo-orchestrator\[lineage\]"
    ):
        source.column_usage(ConsumerAction(id="child", query="SELECT 1 FROM table_orders"))

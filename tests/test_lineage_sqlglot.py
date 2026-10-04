"""sqlglot-backed column usage analysis (issue #13).

Skipped unless the optional `lineage` extra is installed. Everything here is
offline and synthetic: plain BigQuery SQL, no query is ever executed, and no
identifier here is a real table.
"""

import pytest

from kingyo_orchestrator.core.column_usage import ConsumerAction, relevance

pytest.importorskip("sqlglot", reason="install kingyo-orchestrator[lineage]")

from kingyo_orchestrator.lineage import (  # noqa: E402
    SqlglotColumnUsageSource,
    StaticColumnCatalog,
)

ORDERS = "project_x.dataset_a.table_orders"


def usage_of(sql: str, catalog=None):
    return SqlglotColumnUsageSource(catalog).column_usage(ConsumerAction(id="child", query=sql))


def columns_of(sql: str, catalog=None):
    usage = usage_of(sql, catalog)
    assert not usage.unresolved, f"expected a resolvable answer, got: {usage.reason}"
    return {table: set(names) for table, names in usage.by_table.items()}


# ------------------------------------------------------------------ what counts as used
def test_select_list_columns_count_as_used():
    assert columns_of("SELECT order_id, amount FROM table_orders") == {
        "table_orders": {"order_id", "amount"}
    }


def test_filtered_only_column_counts_as_used():
    """A column read only in WHERE can change which rows appear, so it is used."""
    result = columns_of("SELECT order_id FROM table_orders WHERE audit_ts > '2026-01-01'")
    assert result == {"table_orders": {"order_id", "audit_ts"}}


def test_join_on_columns_count_as_used():
    result = columns_of("SELECT a.order_id FROM dataset_a.a a JOIN dataset_a.b b ON a.id = b.a_id")
    assert result == {
        "dataset_a.a": {"order_id", "id"},
        "dataset_a.b": {"a_id"},
    }


def test_join_using_column_counts_as_used_with_a_catalog():
    catalog = StaticColumnCatalog({"dataset_a.a": ("id",), "dataset_a.b": ("id",)})
    result = columns_of("SELECT a.id FROM dataset_a.a a JOIN dataset_a.b b USING (id)", catalog)
    assert result == {"dataset_a.a": {"id"}, "dataset_a.b": {"id"}}, "a USING column names both"


def test_group_by_order_by_having_and_qualify_columns_count_as_used():
    assert columns_of(
        "SELECT region, count(*) AS n FROM table_orders GROUP BY region HAVING n > 1 ORDER BY region"
    ) == {"table_orders": {"region"}}
    assert columns_of(
        "SELECT order_id FROM table_orders "
        "QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY sold_ts) = 1"
    ) == {"table_orders": {"order_id", "customer_id", "sold_ts"}}


def test_window_and_function_arguments_count_as_used():
    result = columns_of("SELECT sum(amount) OVER (PARTITION BY region) AS total FROM table_orders")
    assert result == {"table_orders": {"amount", "region"}}


def test_distinct_and_case_expressions_count_as_used():
    assert columns_of("SELECT DISTINCT customer_id FROM table_orders") == {
        "table_orders": {"customer_id"}
    }
    assert columns_of(
        "SELECT CASE WHEN region = 'EU' THEN amount ELSE 0 END AS net FROM table_orders"
    ) == {"table_orders": {"region", "amount"}}


def test_count_star_reads_no_column():
    """count(*) reads rows, not a column, so a changed column is a no-op for it."""
    assert columns_of("SELECT count(*) AS n FROM table_orders") == {"table_orders": set()}


# ------------------------------------------------------------------ shapes and aliases
def test_table_alias_qualifies_columns():
    result = columns_of("SELECT o.order_id FROM dataset_a.orders AS o")
    assert result == {"dataset_a.orders": {"order_id"}}


def test_column_uses_pass_through_a_cte():
    result = columns_of(
        "WITH recent AS (SELECT order_id, amount FROM table_orders) SELECT order_id FROM recent"
    )
    assert result == {"table_orders": {"order_id", "amount"}}
    assert "recent" not in result, "a CTE name is not a source table"


def test_column_uses_pass_through_a_derived_table():
    result = columns_of(
        "SELECT r.order_id FROM (SELECT order_id, amount FROM table_orders) AS r WHERE r.amount > 0"
    )
    assert result == {"table_orders": {"order_id", "amount"}}


def test_subquery_in_a_predicate_is_analyzed():
    result = columns_of(
        "SELECT order_id FROM table_orders WHERE customer_id IN (SELECT customer_id FROM customers)"
    )
    assert result == {"table_orders": {"order_id", "customer_id"}, "customers": {"customer_id"}}


def test_set_operation_keeps_both_branches():
    result = columns_of("SELECT order_id FROM table_orders UNION ALL SELECT order_id FROM archive")
    assert result == {"table_orders": {"order_id"}, "archive": {"order_id"}}


def test_struct_field_access_reads_the_struct_column():
    assert columns_of("SELECT o.payload.amount FROM table_orders o") == {
        "table_orders": {"payload"}
    }


def test_three_part_reference_is_reported_as_written():
    result = columns_of(f"SELECT order_id FROM {ORDERS}")
    assert result == {ORDERS: {"order_id"}}


# ------------------------------------------------------------------ conservative fallback
@pytest.mark.parametrize(
    "sql, fragment",
    [
        ("SELECT * FROM table_orders", "star"),
        ("SELECT o.* FROM table_orders o", "star"),
        ("SELECT order_id FROM table_orders WHERE (", "could not be parsed"),
        (None, "no query text"),
        ("   ", "no query text"),
    ],
)
def test_unresolvable_queries_use_every_column(sql, fragment):
    usage = usage_of(sql)
    assert usage.unresolved
    assert fragment in usage.reason
    assert usage.uses(ORDERS, "any_column") is None, (
        "an unresolved consumer must count every column"
    )


def test_ambiguous_unqualified_column_is_unresolved():
    usage = usage_of("SELECT region FROM dataset_a.a JOIN dataset_a.b ON a.id = b.a_id")
    assert usage.unresolved
    assert "unqualified" in usage.reason

    catalog = StaticColumnCatalog(
        {"dataset_a.a": ("id", "region"), "dataset_a.b": ("a_id", "region")}
    )
    usage = usage_of("SELECT region FROM dataset_a.a JOIN dataset_a.b ON a.id = b.a_id", catalog)
    assert usage.unresolved
    assert "more than one source" in usage.reason


def test_unknown_alias_is_unresolved_rather_than_guessed():
    usage = usage_of("SELECT missing.order_id FROM table_orders")
    assert usage.unresolved
    assert "alias" in usage.reason


def test_unsupported_from_shapes_are_unresolved():
    for sql, fragment in [
        ("SELECT x FROM table_orders o, UNNEST(o.items) AS x", "not supported"),
        ("SELECT order_id FROM (SELECT order_id FROM table_orders)", "no alias"),
    ]:
        usage = usage_of(sql)
        assert usage.unresolved, sql
        assert fragment in usage.reason, usage.reason


def test_statement_without_a_table_is_unresolved():
    """A constant query reads no table; it is reported rather than trusted as reading nothing."""
    assert usage_of("SELECT 1 AS x").unresolved


# ------------------------------------------------------------------ catalog behaviour
def test_catalog_attributes_an_unqualified_column_to_its_only_declaring_source():
    catalog = StaticColumnCatalog(
        {"dataset_a.a": ("id", "region"), "dataset_a.b": ("a_id", "note")}
    )
    result = columns_of("SELECT region FROM dataset_a.a JOIN dataset_a.b ON a.id = b.a_id", catalog)
    assert result == {"dataset_a.a": {"region", "id"}, "dataset_a.b": {"a_id"}}


def test_catalog_is_optional_and_unknown_tables_return_none():
    catalog = StaticColumnCatalog({"dataset_a.a": ("id",)})
    assert catalog.columns("dataset_a.a") == ("id",)
    assert catalog.columns("dataset_a.missing") is None


# ------------------------------------------------------------------ end to end
def pipeline(*consumers):
    actions = {
        ORDERS: ConsumerAction(id=ORDERS, query="SELECT 1"),
    }
    for identifier, sql in consumers:
        actions[identifier] = ConsumerAction(id=identifier, query=sql, depends_on=(ORDERS,))
    return actions


def test_audit_column_nobody_reads_is_a_no_op_for_every_child():
    actions = pipeline(
        ("dataset_a.table_daily", "SELECT order_id, amount FROM table_orders"),
        ("dataset_a.table_counts", "SELECT count(*) AS n FROM table_orders"),
    )
    results = relevance(actions, ORDERS, ("_fingerprint",), source=SqlglotColumnUsageSource())

    assert {name: result.verdict for name, result in results.items()} == {
        "dataset_a.table_counts": "no_op",
        "dataset_a.table_daily": "no_op",
    }


def test_one_change_can_be_relevant_to_one_child_and_a_no_op_for_another():
    actions = pipeline(
        ("dataset_a.table_daily", "SELECT order_id, amount FROM table_orders"),
        ("dataset_a.table_audit", "SELECT order_id FROM table_orders"),
    )
    results = relevance(actions, ORDERS, ("amount",), source=SqlglotColumnUsageSource())

    assert results["dataset_a.table_daily"].verdict == "relevant"
    assert results["dataset_a.table_daily"].columns == ("amount",)
    assert results["dataset_a.table_audit"].verdict == "no_op"
    assert "amount" in results["dataset_a.table_audit"].reason


def test_column_used_only_in_a_filter_still_counts():
    actions = pipeline(
        (
            "dataset_a.table_recent",
            "SELECT order_id FROM table_orders WHERE updated_ts > '2026-01-01'",
        )
    )
    results = relevance(actions, ORDERS, ("updated_ts",), source=SqlglotColumnUsageSource())
    assert results["dataset_a.table_recent"].verdict == "relevant"


def test_column_used_only_through_a_cte_counts():
    actions = pipeline(
        (
            "dataset_a.table_through_cte",
            "WITH recent AS (SELECT order_id, amount FROM table_orders) SELECT order_id FROM recent",
        )
    )
    source = SqlglotColumnUsageSource()
    assert (
        relevance(actions, ORDERS, ("amount",), source=source)[
            "dataset_a.table_through_cte"
        ].verdict
        == "relevant"
    )
    assert (
        relevance(actions, ORDERS, ("note",), source=source)["dataset_a.table_through_cte"].verdict
        == "no_op"
    )


def test_star_select_child_is_always_relevant():
    actions = pipeline(("dataset_a.table_copy", "SELECT * FROM table_orders"))
    results = relevance(actions, ORDERS, ("note",), source=SqlglotColumnUsageSource())
    assert results["dataset_a.table_copy"].verdict == "relevant"
    assert "star" in results["dataset_a.table_copy"].reason


def test_source_matches_a_graph_id_even_when_the_sql_uses_a_short_reference():
    actions = pipeline(("dataset_a.table_daily", "SELECT order_id FROM table_orders"))
    results = relevance(actions, ORDERS, ("order_id",), source=SqlglotColumnUsageSource())
    assert results["dataset_a.table_daily"].verdict == "relevant"

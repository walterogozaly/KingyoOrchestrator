"""Offline witnesses and boundary checks from audit #47. No provider calls, no cloud.

The audit asks one question in the dangerous direction: can `column_usage` report a
column as unread when the consumer really reads it? It did not find one. What it did find is
that a derived source which cannot be attributed raises instead of returning the
conservative fallback, so the unresolved path is never reached for those shapes and
`relevance` aborts.

Expected failures are strict: a fix causes an unexpected pass until the marker is removed,
rather than silently leaving a resolved witness marked broken.
"""

from __future__ import annotations

import pytest

from kingyo_orchestrator.core.column_usage import ConsumerAction, relevance
from kingyo_orchestrator.lineage import SqlglotColumnUsageSource, StaticColumnCatalog

pytest.importorskip("sqlglot")

ORDERS = "`example-project.analytics.orders`"
CUSTOMER = "`example-project.analytics.customer`"
UNCATALOGUED = "`example-project.analytics.uncatalogued`"
ORDERS_ID = "example-project.analytics.orders"
CATALOG = StaticColumnCatalog(
    {
        "orders": ("id", "custkey", "amount", "ts", "flag", "address"),
        "customer": ("custkey", "name"),
    }
)


def usage(query: str):
    source = SqlglotColumnUsageSource(CATALOG)
    return source.column_usage(ConsumerAction(id="c", query=query, depends_on=()))


def columns(result, table: str = "orders") -> set[str]:
    """Columns the analysis attributes to `table`, however it was spelled in the query."""
    found: set[str] = set()
    for reference, names in result.by_table.items():
        if reference.rsplit(".", 1)[-1].casefold() == table:
            found |= {name.casefold() for name in names}
    return found


def consumer(name: str, query: str, depends_on=(ORDERS_ID,)) -> ConsumerAction:
    """A child of `ORDERS_ID`, which is what `relevance` walks."""
    return ConsumerAction(id=name, query=query, depends_on=tuple(depends_on))


# --------------------------------------------------------------------------- the finding

DERIVED_CASES = [
    ("inner_star", f"SELECT o.amount FROM (SELECT * FROM {ORDERS}) o"),
    ("inner_qualified_star", f"SELECT o.amount FROM (SELECT x.* FROM {ORDERS} x) o"),
    ("inner_bad_alias", f"SELECT o.amount FROM (SELECT zz.id FROM {ORDERS} x) o"),
    (
        "inner_ambiguous_column",
        f"SELECT o.amount FROM (SELECT id FROM {ORDERS} x JOIN {UNCATALOGUED} y ON y.k = x.k) o",
    ),  # noqa: E501
    (
        "inner_nested_derived",
        f"SELECT o.amount FROM (SELECT * FROM (SELECT amount FROM {ORDERS}) a) o",
    ),
    (
        "cte_with_star_in_from",
        f"SELECT o.amount FROM (WITH s AS (SELECT * FROM {ORDERS}) SELECT amount FROM s) o",
    ),  # noqa: E501
]
DERIVED_IDS = [case[0] for case in DERIVED_CASES]


@pytest.mark.xfail(
    strict=True,
    raises=AttributeError,
    reason="audit #47: a derived source that errors crashes instead of returning all_used",
)
@pytest.mark.parametrize(("label", "query"), DERIVED_CASES, ids=DERIVED_IDS)
def test_a_derived_source_that_cannot_be_attributed_falls_back(label, query):
    result = usage(query)
    assert result.unresolved, "an unattributable derived source must be reported as unresolved"
    assert result.reason


@pytest.mark.xfail(
    strict=True,
    raises=AttributeError,
    reason="audit #47: relevance does not survive a lineage backend that raises",
)
def test_relevance_reports_an_unattributable_child_as_relevant():
    child = consumer(
        "example-project.analytics.m", f"SELECT o.amount FROM (SELECT * FROM {ORDERS}) o"
    )
    answers = relevance(
        {child.id: child}, ORDERS_ID, ["amount"], source=SqlglotColumnUsageSource(CATALOG)
    )
    assert answers[child.id].relevant


# --------------------------------------------------------------------------- control


def test_an_attributable_derived_source_still_resolves():
    result = usage(f"SELECT o.amount FROM (SELECT amount FROM {ORDERS}) o")
    assert not result.unresolved, result.reason
    assert columns(result) == {"amount"}


def test_a_renamed_derived_column_is_traced_back_to_the_source_column():
    result = usage(f"SELECT s.b FROM (SELECT amount AS b FROM {ORDERS}) s")
    assert columns(result) == {"amount"}


# --------------------------------------------------------------------------- the direction that matters

#: (label, query, table, column the consumer really reads). A false `no_op` is the worst failure here,
#: so this table is the important half of the audit.
NO_OP_RISK_CASES = [
    ("select_list", f"SELECT o.id, o.amount FROM {ORDERS} o", "orders", "amount"),
    ("where_only", f"SELECT id FROM {ORDERS} WHERE flag", "orders", "flag"),
    (
        "join_on",
        f"SELECT o.id FROM {ORDERS} o JOIN {CUSTOMER} c ON c.custkey = o.custkey",
        "orders",
        "custkey",
    ),
    (
        "join_using",
        f"SELECT o.amount FROM {ORDERS} o JOIN {CUSTOMER} c USING (custkey)",
        "orders",
        "custkey",
    ),
    (
        "join_on_expression",
        f"SELECT o.id FROM {ORDERS} o JOIN {CUSTOMER} c ON concat(c.name, 'x') = o.flag",
        "orders",
        "flag",
    ),
    (
        "group_by_ordinal",
        f"SELECT custkey FROM {ORDERS} GROUP BY 1 HAVING sum(amount) > 1",
        "orders",
        "amount",
    ),
    ("order_by", f"SELECT id FROM {ORDERS} ORDER BY ts DESC", "orders", "ts"),
    (
        "window_frame",
        f"SELECT sum(amount) OVER (PARTITION BY custkey ORDER BY ts) s FROM {ORDERS}",
        "orders",
        "amount",
    ),
    (
        "qualify",
        f"SELECT id, row_number() OVER (PARTITION BY custkey ORDER BY ts) rn "
        f"FROM {ORDERS} QUALIFY rn = 1",
        "orders",
        "ts",
    ),
    ("struct_field", f"SELECT o.address.city AS city FROM {ORDERS} o", "orders", "address"),
    ("array_agg", f"SELECT ARRAY_AGG(o.amount) a FROM {ORDERS} o", "orders", "amount"),
    (
        "group_key_expression",
        f"SELECT DATE(ts) d, count(*) FROM {ORDERS} GROUP BY DATE(ts)",
        "orders",
        "ts",
    ),
    (
        "union_all",
        f"SELECT amount FROM {ORDERS} UNION ALL SELECT amount FROM {ORDERS}",
        "orders",
        "amount",
    ),
    (
        "cte_passthrough",
        f"WITH s AS (SELECT id, amount FROM {ORDERS}) SELECT amount FROM s",
        "orders",
        "amount",
    ),
    (
        "nested_cte_chain",
        f"WITH a AS (SELECT id, amt FROM {ORDERS}), "
        "b AS (SELECT id, sum(amt) s FROM a GROUP BY id) SELECT id, s FROM b",
        "orders",
        "amt",
    ),
    (
        "derived_subquery",
        f"SELECT s.amount FROM {ORDERS} o JOIN (SELECT custkey, amount FROM {ORDERS}) s "
        "ON s.custkey = o.custkey",
        "orders",
        "amount",
    ),
    (
        "scalar_subquery",
        f"SELECT id, (SELECT max(amount) FROM {ORDERS}) m FROM {ORDERS}",
        "orders",
        "amount",
    ),
    (
        "cte_shadows_table",
        f"WITH orders AS (SELECT custkey FROM {ORDERS}) SELECT custkey FROM orders",
        "orders",
        "custkey",
    ),
]


@pytest.mark.parametrize(
    ("label", "query", "table", "column"),
    NO_OP_RISK_CASES,
    ids=[case[0] for case in NO_OP_RISK_CASES],
)
def test_a_read_column_is_never_reported_as_unread(label, query, table, column):
    result = usage(query)
    assert not result.unresolved, f"{label} unexpectedly unresolved: {result.reason}"
    assert column in columns(result, table), f"{label} did not report {table}.{column}"


def test_an_unread_column_is_reported_as_a_no_op():
    child = consumer("example-project.analytics.m", f"SELECT o.id FROM {ORDERS} o")
    answers = relevance(
        {child.id: child}, ORDERS_ID, ["flag"], source=SqlglotColumnUsageSource(CATALOG)
    )
    assert answers[child.id].verdict == "no_op"
    assert not answers[child.id].relevant
    assert answers[child.id].reason


def test_a_table_the_analysis_never_saw_is_relevant_not_a_no_op():
    """A child whose SQL never names the changed table must not be able to skip work."""
    child = consumer("example-project.analytics.m", f"SELECT o.id FROM {CUSTOMER} o")
    answers = relevance(
        {child.id: child}, ORDERS_ID, ["flag"], source=SqlglotColumnUsageSource(CATALOG)
    )
    assert answers[child.id].relevant
    assert "did not see" in answers[child.id].reason


def test_name_matching_widens_rather_than_narrows():
    """`by_table` is matched by table name, so the same name in another project still counts as read."""
    other = "other-project.analytics.orders"
    child = consumer("example-project.analytics.m", f"SELECT o.flag FROM {ORDERS} o", [other])
    answers = relevance(
        {child.id: child}, other, ["flag"], source=SqlglotColumnUsageSource(CATALOG)
    )
    assert answers[child.id].relevant


# --------------------------------------------------------------------------- documented gaps

#: Both err toward `relevant`, so neither can skip work. Recorded so the cost stays visible.
CONSERVATIVE_GAPS = [
    (
        "correlated_exists",
        f"SELECT o.id FROM {ORDERS} o WHERE EXISTS "
        f"(SELECT 1 FROM {CUSTOMER} c WHERE c.custkey = o.custkey)",
    ),
    (
        "correlated_scalar_subquery",
        f"SELECT id, (SELECT name FROM {CUSTOMER} WHERE custkey = o.custkey) nm FROM {ORDERS} o",
    ),
    (
        "cte_joined_to_itself",
        f"WITH s AS (SELECT custkey, amount FROM {ORDERS}) "
        "SELECT 1 FROM s a JOIN s b ON a.amount = b.amount",
    ),
]


@pytest.mark.parametrize(
    ("label", "query"),
    CONSERVATIVE_GAPS,
    ids=[case[0] for case in CONSERVATIVE_GAPS],
)
def test_known_gaps_stay_conservative(label, query):
    result = usage(query)
    assert result.unresolved, f"{label} became resolvable; re-check whether it is still safe"
    assert result.reason

"""Shapes that must NOT be classified as partition- or key-local (bugs reported by the flattening thread)."""

from kingyo_orchestrator.incremental.analysis import analyze_edge
from kingyo_orchestrator.incremental.sqlx import Model

P = Model("orders", "declaration", "", partition_expr="DATE(last_upd_ts)")
COLS = {"orders": ["order_id", "customer_id", "amount", "last_upd_ts"]}


def kind(sql, part="DATE(last_upd_ts)"):
    return analyze_edge(Model("m", "table", sql, ["orders"], part), P, COLS).kind


def test_cte_referenced_twice_is_not_keyed():
    sql = """with b as (select * from orders)
             select x.customer_id, max(x.last_upd_ts) as last_upd_ts, count(*) as n
             from b x join b y on x.customer_id = y.customer_id group by x.customer_id"""
    assert kind(sql) == "full"


def test_where_subquery_over_parent_is_not_local():
    sql = """select order_id, customer_id, amount, last_upd_ts from orders
             where amount > (select avg(amount) from orders)"""
    assert kind(sql) == "full"


def test_keyed_still_detected():
    sql = """with b as (select * from orders)
             select customer_id, max(last_upd_ts) as last_upd_ts, sum(amount) as amount from b group by customer_id"""
    assert kind(sql) == "keyed"

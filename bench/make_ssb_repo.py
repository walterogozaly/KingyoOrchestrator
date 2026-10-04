"""Writes ssb_repo/: a Dataform-style SSB pipeline (plain SQLX, supported style) from the 13 SSB flights.

SSB is query-level; Kingyo needs a pipeline. Layout:
  sources (declarations): lineorder (mutable, uniqueKey lo_rowid, partitioned by load date), customer, supplier, part, dates
  stg_lineorder   partition DATE(lo_loaded_ts)   row-local cleanup                      (aligned with source)
  fact_sales      partition order_date           star join of stg_lineorder to the 4 dimensions
  daily_revenue   partition order_date           GROUP BY order_date (+dims)
  q1_1 .. q4_3    partition d_year               the 13 SSB flights, adapted: every flight is grouped by d_year
                                                 (Q1.x gain a d_year grouping) so each mart is partitionable by year.
These are "SSB-derived", not the official queries (Q1.x differ), and are labelled so in reports.
"""

from pathlib import Path

OUT = Path(__file__).parent / "ssb_repo" / "definitions"
OUT.mkdir(parents=True, exist_ok=True)


def w(name, cfg, body):
    (OUT / f"{name}.sqlx").write_text(f"config {{ {cfg} }}\n\n{body.strip()}\n")


for n in ["lineorder", "customer", "supplier", "part", "dates"]:
    extra = (
        ', uniqueKey: "lo_rowid", bigquery: { partitionBy: "DATE(lo_loaded_ts)" }'
        if n == "lineorder"
        else ""
    )
    w(n, f'type: "declaration", name: "{n}"{extra}', "")
    (OUT / f"{n}.sqlx").write_text(f'config {{ type: "declaration", name: "{n}"{extra} }}\n')

w(
    "stg_lineorder",
    'type: "table", bigquery: { partitionBy: "DATE(lo_loaded_ts)" }',
    """
select lo_rowid, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate as order_date, lo_loaded_ts,
       lo_quantity, lo_extendedprice, lo_discount, lo_revenue, lo_supplycost
from ${ref("lineorder")}
where lo_quantity > 0
""",
)

w(
    "fact_sales",
    'type: "table", bigquery: { partitionBy: "order_date" }',
    """
select l.lo_rowid, l.order_date, l.lo_loaded_ts, d.d_year, d.d_yearmonthnum, d.d_yearmonth, d.d_weeknuminyear,
       c.c_nation, c.c_region, c.c_city, s.s_nation, s.s_region, s.s_city,
       p.p_mfgr, p.p_category, p.p_brand1,
       l.lo_quantity, l.lo_extendedprice, l.lo_discount, l.lo_revenue, l.lo_supplycost
from ${ref("stg_lineorder")} l
join ${ref("customer")} c on l.lo_custkey = c.c_custkey
join ${ref("supplier")} s on l.lo_suppkey = s.s_suppkey
join ${ref("part")} p on l.lo_partkey = p.p_partkey
join ${ref("dates")} d on l.order_date = d.d_date
""",
)

w(
    "daily_revenue",
    'type: "table", bigquery: { partitionBy: "order_date" }',
    """
select order_date, c_region, sum(lo_revenue) as revenue, count(*) as n_lines
from ${ref("fact_sales")}
group by order_date, c_region
""",
)

Q = {
    "q1_1": (
        "d_year",
        "sum(lo_extendedprice * lo_discount)",
        "d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25",
    ),
    "q1_2": (
        "d_year",
        "sum(lo_extendedprice * lo_discount)",
        "d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35",
    ),
    "q1_3": (
        "d_year",
        "sum(lo_extendedprice * lo_discount)",
        "d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35",
    ),
    "q2_1": (
        "d_year, p_brand1",
        "sum(lo_revenue)",
        "p_category = 'MFGR#12' and s_region = 'AMERICA'",
    ),
    "q2_2": (
        "d_year, p_brand1",
        "sum(lo_revenue)",
        "p_brand1 between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'",
    ),
    "q2_3": (
        "d_year, p_brand1",
        "sum(lo_revenue)",
        "p_brand1 = 'MFGR#2239' and s_region = 'EUROPE'",
    ),
    "q3_1": (
        "c_nation, s_nation, d_year",
        "sum(lo_revenue)",
        "c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1997",
    ),
    "q3_2": (
        "c_city, s_city, d_year",
        "sum(lo_revenue)",
        "c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997",
    ),
    "q3_3": (
        "c_city, s_city, d_year",
        "sum(lo_revenue)",
        "c_city in ('UNITED KI1', 'UNITED KI5') and s_city in ('UNITED KI1', 'UNITED KI5') and d_year between 1992 and 1997",
    ),
    "q3_4": (
        "c_city, s_city, d_year",
        "sum(lo_revenue)",
        "c_city in ('UNITED KI1', 'UNITED KI5') and s_city in ('UNITED KI1', 'UNITED KI5') and d_yearmonth = 'Dec1997'",
    ),
    "q4_1": (
        "d_year, c_nation",
        "sum(lo_revenue - lo_supplycost)",
        "c_region = 'AMERICA' and s_region = 'AMERICA' and p_mfgr in ('MFGR#1', 'MFGR#2')",
    ),
    "q4_2": (
        "d_year, s_nation, p_category",
        "sum(lo_revenue - lo_supplycost)",
        "c_region = 'AMERICA' and s_region = 'AMERICA' and d_year in (1997, 1998) and p_mfgr in ('MFGR#1', 'MFGR#2')",
    ),
    "q4_3": (
        "d_year, s_city, p_brand1",
        "sum(lo_revenue - lo_supplycost)",
        "c_region = 'AMERICA' and s_nation = 'UNITED STATES' and d_year in (1997, 1998) and p_category = 'MFGR#14'",
    ),
}
for name, (grp, agg, where) in Q.items():
    w(
        name,
        'type: "table", bigquery: { partitionBy: "d_year" }',
        f"""
select {grp}, {agg} as measure
from ${{ref("fact_sales")}}
where {where}
group by {grp}
""",
    )
print("wrote", len(list(OUT.glob("*.sqlx"))), "models")

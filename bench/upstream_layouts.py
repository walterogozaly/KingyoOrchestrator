"""Upstream layout variants for the SSB pipeline: how the fact source `lineorder` is partitioned.

  load_ts           (default repo) partitioned by the change column, DATE(lo_loaded_ts): the signal names load dates
  order_date        partitioned by a business date that differs from the change column (the LAST_UPD_TS vs
                    ORDER_SOLD_TS case): the signal names the order dates that the changed rows fall in
  unpartitioned     no partitioning on the source: the signal can only say "table changed" (ALL)

Staging follows the source layout for order_date/unpartitioned (partitioned by order_date). Dimensions stay
unpartitioned in every layout. `remap_signals` turns the scenario's load-date signals into the signal a
change-detection step would emit for the variant.
"""

from __future__ import annotations

import shutil
from pathlib import Path

LAYOUTS = ("load_ts", "order_date", "unpartitioned")
ALL = "ALL"


def make_layout_repo(src: Path, dest: Path, layout: str) -> Path:
    if layout == "load_ts":
        return src
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)
    d = dest / "definitions"
    lo, stg = d / "lineorder.sqlx", d / "stg_lineorder.sqlx"
    part = ', bigquery: { partitionBy: "DATE(lo_loaded_ts)" }'
    new = ', bigquery: { partitionBy: "lo_orderdate" }' if layout == "order_date" else ""
    assert part in lo.read_text()
    lo.write_text(lo.read_text().replace(part, new))
    old = 'partitionBy: "DATE(lo_loaded_ts)"'
    assert old in stg.read_text()
    stg.write_text(stg.read_text().replace(old, 'partitionBy: "order_date"'))
    return dest


def remap_signals(con, signals, layout: str):
    """Scenario signals name load dates; return what the source's own partitioning would signal."""
    if layout == "load_ts":
        return signals
    out = []
    for table, parts, *rest in signals:
        if table != "lineorder" or parts == ALL:
            out.append((table, parts, *rest))
        elif layout == "unpartitioned":
            out.append((table, ALL, *rest))
        else:
            lst = ",".join(f"DATE '{p}'" for p in parts)
            rows = con.execute(
                f"SELECT DISTINCT lo_orderdate FROM lineorder WHERE CAST(lo_loaded_ts AS DATE) IN ({lst})"
            ).fetchall()
            out.append((table, sorted(str(r[0]) for r in rows), *rest))
    return out

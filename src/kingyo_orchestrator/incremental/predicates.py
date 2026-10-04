"""Partition filters the engine can prune on.

`CAST(DATE(ts) AS VARCHAR) IN ('2026-10-03')` is correct but opaque: neither BigQuery nor DuckDB can
skip partitions/row groups through it. `part_pred` emits the same condition in a prunable shape:
  DATE(col) / CAST(col AS DATE)  ->  col >= TIMESTAMP 'd 00:00:00' AND col < TIMESTAMP 'd+1 00:00:00'
                                     (consecutive days merged into one range)
  anything else                  ->  <expr> IN ('v', ...)   (literal coerced to the expression's type)
"""

from __future__ import annotations

import re
from datetime import date, timedelta

from sqlglot import exp, parse_one

_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _lit(vs):
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in vs)


def _day_column(expr: str) -> str | None:
    try:
        e = parse_one(expr)
    except Exception:
        return None
    if (
        isinstance(e, exp.Date)
        and isinstance(e.this, exp.Column)
        and len([a for a in e.args.values() if a]) == 1
    ):
        return e.this.sql()
    if isinstance(e, exp.Cast) and e.to.is_type("date") and isinstance(e.this, exp.Column):
        return e.this.sql()
    return None


def part_pred(expr: str, values) -> str:
    vals = sorted({str(v) for v in values if v is not None})
    nulls = any(v is None for v in values)
    if not vals and not nulls:
        return "false"
    col = _day_column(expr)
    if col and vals and all(_DAY.match(v) for v in vals):
        days = [date.fromisoformat(v) for v in vals]
        ranges, start, prev = [], days[0], days[0]
        for d in days[1:]:
            if d != prev + timedelta(days=1):
                ranges.append((start, prev))
                start = d
            prev = d
        ranges.append((start, prev))
        pred = " OR ".join(
            f"({col} >= TIMESTAMP '{a} 00:00:00' AND {col} < TIMESTAMP '{b + timedelta(days=1)} 00:00:00')"
            for a, b in ranges
        )
    elif vals and all(_DAY.match(v) for v in vals):
        # partitionBy expressions are DATE/TIMESTAMP/INT64 in BigQuery: a typed literal list is statically prunable
        pred = f"{expr} IN ({', '.join(f'DATE {chr(39)}{v}{chr(39)}' for v in vals)})"
    else:
        pred = f"{expr} IN ({_lit(vals)})" if vals else "false"
    if nulls:
        pred = f"({pred} OR {expr} IS NULL)"
    return f"({pred})"


def value_list(values) -> str:
    """Python values (as fetched from the warehouse) -> SQL literal list for `col IN (...)`."""
    import datetime as _dt
    from decimal import Decimal

    out = []
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            out.append("TRUE" if v else "FALSE")
        elif isinstance(v, (int, float, Decimal)):
            out.append(str(v))
        elif isinstance(v, _dt.datetime):
            out.append(f"TIMESTAMP '{v.isoformat(sep=' ')}'")
        elif isinstance(v, _dt.date):
            out.append(f"DATE '{v.isoformat()}'")
        else:
            out.append("'" + str(v).replace("'", "''") + "'")
    return ", ".join(out) or "NULL"

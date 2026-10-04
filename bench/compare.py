"""Result comparison / correctness checker for the benchmark harness.

One tested function decides "candidate output equals baseline output", so the harness never hands out
speed credit for wrong results.

    compare_tables(con_a, con_b, table, *, ordered_by=None, float_tol=1e-9) -> list[str]
    compare_dag(con_a, con_b, tables, **kw) -> dict[str, list[str]]

`con_a` and `con_b` are DuckDB connections (side a = baseline, side b = candidate). `table` is either one
name looked up on both connections, or a `(name_a, name_b)` pair for two tables that share a connection.

Rules
- Same column names and the same order, otherwise a "columns differ" problem.
- Default is multiset equality: duplicate counts matter.
- NULL equals NULL, NaN equals NaN, floats equal within relative `float_tol`; timestamps, dates and
  decimals compare exactly (they are not float types).
- With `ordered_by`, rows must match in that order, and rows tied on `ordered_by` are compared as
  multisets, so ordering ties are never reported as a difference. Both sides are sorted by
  `ordered_by` first and by content second, which canonicalises ties; for two tables that makes the
  ordered mode equivalent to multiset equality, and it only bites for callers that materialise an
  ordered result set as a table.
- At most 5 sample differing rows are reported per table.

Memory
- Both tables in one connection (`con_a is con_b`) and no tolerance needed: one `EXCEPT ALL` query
  decides equality, so no row ever reaches Python.
- Otherwise DuckDB sorts both sides and the checker streams them in chunks, merging one row per side at a
  time, so a large table is never materialised as a Python list.

Standalone on purpose: `bench/harness.py` keeps its own `compare` until the harness owner swaps it for a
call to `compare_dag`.
"""

from __future__ import annotations

import math
import re
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

__all__ = ["compare_tables", "compare_dag"]

#: Maximum number of sample differing rows reported per table.
SAMPLE_LIMIT = 5
#: Rows pulled from DuckDB per `fetchmany` call while streaming.
CHUNK_ROWS = 50_000
#: DuckDB types compared with `float_tol`; everything else (dates, decimals, strings) compares exactly.
FLOAT_TYPES = frozenset({"FLOAT", "DOUBLE", "REAL"})
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
_MAX_SHOWN = 60


# --------------------------------------------------------------------------- shape


class _Shape:
    """Column names and types of one side of a comparison."""

    def __init__(self, names: Sequence[str], types: Sequence[str]) -> None:
        self.names: tuple[str, ...] = tuple(names)
        self.types: tuple[str, ...] = tuple(str(t) for t in types)

    @property
    def floats(self) -> tuple[int, ...]:
        return tuple(i for i, t in enumerate(self.types) if t.upper() in FLOAT_TYPES)

    @property
    def exact(self) -> tuple[int, ...]:
        keep = set(self.floats)
        return tuple(i for i in range(len(self.names)) if i not in keep)


def _describe(con, table: str) -> _Shape:
    rows = con.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
    return _Shape([r[0] for r in rows], [r[1] for r in rows])


# --------------------------------------------------------------------------- value tokens


def _key_token(value: Any) -> tuple:
    """Total order for a non-float column; NULL first, matching `ORDER BY ... NULLS FIRST`."""
    return (0,) if value is None else (1, value)


def _floats_close(left: Any, right: Any, tol: float) -> bool:
    """Relative-tolerance equality, with NULL == NULL and NaN == NaN."""
    if left is None or right is None:
        return left is None and right is None
    left_nan = isinstance(left, float) and math.isnan(left)
    right_nan = isinstance(right, float) and math.isnan(right)
    if left_nan or right_nan:
        return left_nan and right_nan
    if left == right:
        return True
    if math.isinf(left) or math.isinf(right):
        return False
    return abs(left - right) <= tol * max(abs(left), abs(right))


def _show(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= _MAX_SHOWN else text[: _MAX_SHOWN - 3] + "..."


def _row_text(row: Sequence[Any], indices: Sequence[int] | None = None) -> str:
    cells = tuple(row) if indices is None else tuple(row[i] for i in indices)
    return "(" + ", ".join(_show(v) for v in cells) + ")"


# --------------------------------------------------------------------------- streaming sides


def _cursor_for(con):
    """An independent cursor on `con` with the same search path, so two sides can be read interleaved.

    `cursor()` copies the attached databases but not the active catalog/schema, so a harness that does
    `USE some_db` (as `bench/harness.py` does) needs it restored here. Connections without cursors, such
    as the harness `Meter` proxy, are used as they are.
    """
    if not hasattr(con, "cursor"):
        return con
    cursor = con.cursor()
    try:
        catalog, schema = con.execute("SELECT current_database(), current_schema()").fetchone()
        cursor.execute(
            'USE "{}"."{}"'.format(str(catalog).replace('"', '""'), str(schema).replace('"', '""'))
        )
    except Exception:  # noqa: BLE001 - fall back to the connection itself
        return con
    return cursor


class _Side:
    """One side of a streaming comparison: rows come from DuckDB already sorted, in chunks.

    Each side owns its own cursor, so the two sides can be read interleaved even when they share one
    connection.
    """

    def __init__(
        self,
        con,
        table: str,
        shape: _Shape,
        ordered_by: Sequence[str] = (),
        chunk: int = CHUNK_ROWS,
    ) -> None:
        self._cursor = _cursor_for(con)
        keys = list(ordered_by) + [c for c in shape.names if c not in set(ordered_by)]
        self._query = f"SELECT * FROM {table} ORDER BY " + ", ".join(
            f'"{c}" NULLS FIRST' for c in keys
        )
        self._rows: Iterator[tuple] = self._fetch(chunk)
        self._buffer: deque[tuple] = deque()
        self.seen = 0

    def _fetch(self, chunk: int) -> Iterator[tuple]:
        self._cursor.execute(self._query)
        while True:
            rows = self._cursor.fetchmany(chunk)
            if not rows:
                return
            yield from rows

    def peek(self) -> tuple | None:
        if not self._buffer:
            try:
                self._buffer.append(next(self._rows))
            except StopIteration:
                return None
        return self._buffer[0]

    def pop(self) -> tuple | None:
        row = self.peek()
        if row is None:
            return None
        self._buffer.popleft()
        self.seen += 1
        return row


# --------------------------------------------------------------------------- stream merge


def _key(row: Sequence[Any], indices: Sequence[int]) -> tuple:
    return tuple(_key_token(row[i]) for i in indices)


def _raw_floats(row: Sequence[Any], indices: Sequence[int]) -> tuple:
    return tuple(row[i] for i in indices)


def _drop(
    side: _Side, key_idx: Sequence[int], want: tuple | None, out: list[str], side_name: str
) -> int:
    """Consume rows of group `want`; `want=None` consumes everything left on that side."""
    dropped = 0
    while True:
        row = side.peek()
        if row is None or (want is not None and _key(row, key_idx) != want):
            break
        side.pop()
        dropped += 1
        if len(out) < SAMPLE_LIMIT:
            out.append(f"row only in {side_name}: {_row_text(row)}")
    return dropped


def _merge_group(
    sa: _Side,
    sb: _Side,
    key_idx: Sequence[int],
    fidx: Sequence[int],
    tol: float,
    out: list[str],
) -> tuple[int, int, int]:
    """Compare one group of rows that share a key, matching float tuples greedily in sorted order.

    Both sides are already sorted by key then by float columns, so head matching is optimal for relative
    tolerance. A pair that does not match within tolerance is counted as a value difference and both
    rows are consumed, which keeps the counts right and never hides one. Returns (only in a, only in b,
    values differing beyond tol).
    """
    only_a = only_b = mismatch = 0
    group: tuple | None = None
    while True:
        ra, rb = sa.peek(), sb.peek()
        if ra is None or rb is None:
            break
        group = _key(ra, key_idx)
        if group != _key(rb, key_idx):
            break
        raw_a, raw_b = _raw_floats(ra, fidx), _raw_floats(rb, fidx)
        sa.pop()
        sb.pop()
        if all(_floats_close(x, y, tol) for x, y in zip(raw_a, raw_b, strict=True)):
            continue
        mismatch += 1
        if len(out) < SAMPLE_LIMIT:
            out.append(
                f"row values differ for key {_row_text(ra, key_idx)}: "
                f"a={raw_a!r} b={raw_b!r} (float_tol={tol:g})"
            )
    # At least one side has moved past the group; drain whatever is left of it on the other side.
    only_a += _drop(sa, key_idx, group, out, "a")
    only_b += _drop(sb, key_idx, group, out, "b")
    return only_a, only_b, mismatch


def _stream_problems(sa: _Side, sb: _Side, shape: _Shape, tol: float, label: str) -> list[str]:
    key_idx = shape.exact
    fidx = shape.floats
    samples: list[str] = []
    only_a = only_b = mismatch = 0
    while True:
        ra, rb = sa.peek(), sb.peek()
        if ra is None and rb is None:
            break
        if rb is None:
            only_a += _drop(sa, key_idx, None, samples, "a")
            continue
        if ra is None:
            only_b += _drop(sb, key_idx, None, samples, "b")
            continue
        ka, kb = _key(ra, key_idx), _key(rb, key_idx)
        if ka == kb:
            group = _merge_group(sa, sb, key_idx, fidx, tol, samples)
            only_a += group[0]
            only_b += group[1]
            mismatch += group[2]
        elif ka < kb:
            only_a += _drop(sa, key_idx, ka, samples, "a")
        else:
            only_b += _drop(sb, key_idx, kb, samples, "b")

    if not (only_a or only_b or mismatch):
        return []
    lines: list[str] = []
    if sa.seen != sb.seen:
        lines.append(f"{label}: row counts differ (a={sa.seen}, b={sb.seen})")
    if only_a or only_b:
        lines.append(f"{label}: rows differ ({only_a} only in a, {only_b} only in b)")
    if mismatch:
        lines.append(f"{label}: {mismatch} row value(s) differ beyond float_tol={tol:g}")
    lines.extend(samples[:SAMPLE_LIMIT])
    return lines


# --------------------------------------------------------------------------- same-connection SQL


def _sql_order(shape: _Shape, ordered_by: Sequence[str]) -> str:
    """`ordered_by` first, then every remaining column: a total order with explicit NULL placement."""
    keys = list(ordered_by) + [c for c in shape.names if c not in ordered_by]
    return " ORDER BY " + ", ".join(f'"{c}" NULLS FIRST' for c in keys)


def _sql_problems(
    con, name_a: str, name_b: str, shape: _Shape, ordered_by: Sequence[str], label: str
) -> list[str]:
    columns = ", ".join(f'"{c}"' for c in shape.names)

    def count_diff(x: str, y: str, cols: str) -> str:
        return f"SELECT count(*) FROM (SELECT {cols} FROM {x} EXCEPT ALL SELECT {cols} FROM {y})"

    def sample_diff(x: str, y: str, cols: str) -> str:
        return f"SELECT {cols} FROM {x} EXCEPT ALL SELECT {cols} FROM {y} LIMIT {SAMPLE_LIMIT}"

    if ordered_by:
        # Row numbers make two sorted streams comparable positionally. Sorting by every column first
        # means rows tied on `ordered_by` line up by content, so a tie is never reported as a difference.
        order = _sql_order(shape, ordered_by)
        ranked_a = f"(SELECT row_number() OVER ({order}) AS __rn, {columns} FROM {name_a})"
        ranked_b = f"(SELECT row_number() OVER ({order}) AS __rn, {columns} FROM {name_b})"
        pos_a = f"(SELECT __rn, {columns} FROM {ranked_a})"
        pos_b = f"(SELECT __rn, {columns} FROM {ranked_b})"
        side_a, side_b = pos_a, pos_b
        cols = "*"
    else:
        side_a, side_b, cols = name_a, name_b, columns

    n_a = con.execute(count_diff(side_a, side_b, cols)).fetchone()[0]
    n_b = con.execute(count_diff(side_b, side_a, cols)).fetchone()[0]
    if not n_a and not n_b:
        return []

    lines = [f"{label}: rows differ ({n_a} only in a, {n_b} only in b)"]
    samples: list[str] = []
    for x, y, side in ((side_a, side_b, "a"), (side_b, side_a, "b")):
        if len(samples) >= SAMPLE_LIMIT:
            break
        for row in con.execute(sample_diff(x, y, columns)).fetchall():
            if len(samples) >= SAMPLE_LIMIT:
                break
            samples.append(f"row only in {side}: {_row_text(row)}")
    lines.extend(samples)
    return lines


# --------------------------------------------------------------------------- public API


def _resolve_names(table: str | Sequence[str]) -> tuple[str, str]:
    if isinstance(table, str):
        return table, table
    names = list(table)
    if len(names) != 2:
        raise ValueError("table must be a name or a (name_a, name_b) pair")
    return str(names[0]), str(names[1])


def _unreadable(label: str, side: str, exc: Exception) -> list[str]:
    detail = str(exc).splitlines()[0][:_MAX_SHOWN] if str(exc) else type(exc).__name__
    return [f"{label}: cannot read table on side {side}: {type(exc).__name__}: {detail}"]


def compare_tables(
    con_a,
    con_b,
    table: str | Sequence[str],
    *,
    ordered_by: Sequence[str] | str | None = None,
    float_tol: float = 1e-9,
) -> list[str]:
    """Return an empty list when both sides are equal, otherwise human-readable problems.

    `ordered_by` is a column or list of columns whose order must also match; rows tied on it are
    compared as multisets. `float_tol` is a relative tolerance for FLOAT/DOUBLE/REAL columns; passing
    `0` compares them exactly.
    """
    if float_tol < 0:
        raise ValueError("float_tol must be >= 0")
    name_a, name_b = _resolve_names(table)
    label = name_a if name_a == name_b else f"{name_a} vs {name_b}"
    for name in (name_a, name_b):
        if not _IDENT.match(name):
            return [f"{label}: {name!r} is not a plain table identifier"]

    ordered = (ordered_by,) if isinstance(ordered_by, str) else tuple(ordered_by or ())
    try:
        shape_a = _describe(con_a, name_a)
    except Exception as exc:  # noqa: BLE001 - a driver error is a result, not a crash
        return _unreadable(label, "a", exc)
    try:
        shape_b = _describe(con_b, name_b)
    except Exception as exc:  # noqa: BLE001
        return _unreadable(label, "b", exc)

    if shape_a.names != shape_b.names:
        return [f"{label}: columns differ (a={list(shape_a.names)}, b={list(shape_b.names)})"]
    unknown = [c for c in ordered if c not in shape_a.names]
    if unknown:
        return [f"{label}: ordered_by column(s) not in table: {unknown}"]

    shape = shape_a
    tolerant = float_tol > 0 and bool(shape.floats)
    if con_a is con_b and not tolerant:
        return _sql_problems(con_a, name_a, name_b, shape, ordered, label)

    sa = _Side(con_a, name_a, shape, ordered)
    sb = _Side(con_b, name_b, shape, ordered)
    return _stream_problems(sa, sb, shape, float_tol, label)


def _specs(tables) -> Iterator[tuple[str, Sequence[str] | None]]:
    if isinstance(tables, Mapping):
        for name, order in tables.items():
            yield str(name), order
        return
    for entry in tables:
        if isinstance(entry, str):
            yield entry, None
        else:
            name, order = entry
            yield str(name), order


def compare_dag(con_a, con_b, tables, **kw) -> dict[str, list[str]]:
    """Compare several tables and report per-table problems; every requested table is in the result.

    `tables` is a mapping of table name to `ordered_by`, or an iterable of names or of
    `(name, ordered_by)` pairs. Keyword arguments are passed to `compare_tables`.
    """
    return {
        name: compare_tables(con_a, con_b, name, ordered_by=order, **kw)
        for name, order in _specs(tables)
    }  # noqa: E501

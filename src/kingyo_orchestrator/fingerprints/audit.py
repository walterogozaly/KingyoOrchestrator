"""Conservative, config-independent timestamp candidate detection over offline rows."""

from collections.abc import Hashable, Mapping
from dataclasses import dataclass, replace
from typing import Literal

from .model import _COLUMN, FingerprintConfig, _identifier

Rows = Mapping[Hashable, Mapping[str, object]]


@dataclass(frozen=True)
class SnapshotPair:
    previous: Rows
    current: Rows


@dataclass(frozen=True)
class AuditCandidate:
    column: str
    confidence: Literal["high", "low"]
    reason: str


def propose_audit_columns(
    previous: Rows, current: Rows, column_types: Mapping[str, str]
) -> tuple[AuditCandidate, ...]:
    if not all(isinstance(value, Mapping) for value in (previous, current, column_types)):
        raise ValueError("snapshots and column_types must be mappings")
    for name, data_type in column_types.items():
        _identifier(name, _COLUMN, "declared column")
        if not isinstance(data_type, str) or not data_type:
            raise ValueError("declared column types must be nonempty strings")
    if len(column_types) != len({name.casefold() for name in column_types}):
        raise ValueError("declared column names must not contain case-insensitive duplicates")
    declared = set(column_types)
    for rows in (previous, current):
        for row in rows.values():
            if not isinstance(row, Mapping) or set(row) != declared:
                raise ValueError("snapshot rows must contain exactly the declared columns")
    if set(previous) != set(current) or len(previous) < 2:
        return ()
    candidates = []
    for column in sorted(declared):
        if column_types[column] not in {"TIMESTAMP", "DATETIME"}:
            continue
        before = [row[column] for row in previous.values()]
        after = [row[column] for row in current.values()]
        if any(value is None for value in before + after):
            continue
        changed = [previous[key][column] != current[key][column] for key in previous]
        other_unchanged = all(
            previous[key][other] == current[key][other]
            for key in previous
            for other in declared - {column}
        )
        if all(changed) and other_unchanged:
            candidates.append(
                AuditCandidate(
                    column,
                    "high",
                    "Changed on every matched row while all other columns stayed equal",
                )
            )
        elif not any(changed) and all(value == before[0] for value in before + after):
            candidates.append(
                AuditCandidate(
                    column, "low", "One unchanged value across all rows in both snapshots"
                )
            )
    return tuple(candidates)


def apply_audit_policy(
    config: FingerprintConfig,
    pairs: tuple[SnapshotPair, ...],
    column_types: Mapping[str, str],
    *,
    auto_apply: bool = False,
) -> FingerprintConfig:
    """Confirmed config wins; opt-in auto exclusion requires two consecutive high pairs."""
    if type(auto_apply) is not bool:
        raise ValueError("auto_apply must be an explicit boolean")
    if not isinstance(pairs, tuple) or not all(isinstance(pair, SnapshotPair) for pair in pairs):
        raise ValueError("pairs must be an immutable tuple of SnapshotPair values")
    if config.audit_columns or not auto_apply or len(pairs) < 2:
        return config
    first, second = pairs[-2:]
    if first.current != second.previous:
        raise ValueError("automatic exclusion requires consecutive snapshot pairs")
    detected = [
        {
            candidate.column
            for candidate in propose_audit_columns(pair.previous, pair.current, column_types)
            if candidate.confidence == "high"
        }
        for pair in (first, second)
    ]
    columns = tuple(sorted(detected[0] & detected[1] & set(config.content_columns)))
    if not columns:
        return config
    return replace(
        config,
        content_columns=tuple(name for name in config.content_columns if name not in columns),
        audit_columns=columns,
    )

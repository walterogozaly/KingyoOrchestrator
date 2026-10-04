"""Plain records and pure decisions; callers supply every observation and size."""

import re
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

_COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PROJECT = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")


def _identifier(value: object, pattern: re.Pattern[str], field: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{field} must be a supported simple identifier")


def _columns(values: tuple[str, ...], field: str) -> None:
    if not isinstance(values, tuple):
        raise ValueError(f"{field} must be an immutable tuple")
    for value in values:
        _identifier(value, _COLUMN, field)
    if len(values) != len({value.casefold() for value in values}):
        raise ValueError(f"{field} must not contain duplicates (case-insensitive)")


def _hash(value: int) -> None:
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise ValueError("hash must be a signed 64-bit integer")


@dataclass(frozen=True)
class FingerprintConfig:
    table: str
    key_columns: tuple[str, ...]
    content_columns: tuple[str, ...]
    audit_columns: tuple[str, ...]
    size_threshold_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.table, str) or len(parts := self.table.split(".")) != 3:
            raise ValueError("table must be an explicit project.dataset.table")
        for part, pattern in zip(parts, (_PROJECT, _COLUMN, _COLUMN), strict=True):
            _identifier(part, pattern, "table component")
        for field in ("key_columns", "content_columns", "audit_columns"):
            _columns(getattr(self, field), field)
        if not self.key_columns:
            raise ValueError("key_columns must not be empty")
        if any(name.casefold() == "row_hash" for name in self.key_columns):
            raise ValueError("row_hash is reserved for the per-key hash output, not a key")
        all_columns = self.key_columns + self.content_columns + self.audit_columns
        if len(all_columns) != len({name.casefold() for name in all_columns}):
            raise ValueError("key, content, and audit columns must be disjoint")
        if type(self.size_threshold_bytes) is not int or self.size_threshold_bytes <= 0:
            raise ValueError("size_threshold_bytes must be a positive integer")

    @property
    def columns_hashed(self) -> tuple[str, ...]:
        return self.key_columns + tuple(sorted(self.content_columns))


@dataclass(frozen=True)
class Fingerprint:
    hash: int
    row_count: int
    columns_hashed: tuple[str, ...]
    computed_at: str

    def __post_init__(self) -> None:
        _hash(self.hash)
        if type(self.row_count) is not int or self.row_count < 0:
            raise ValueError("row_count must be a nonnegative integer")
        _columns(self.columns_hashed, "columns_hashed")
        if not self.columns_hashed:
            raise ValueError("columns_hashed must not be empty")
        if not isinstance(self.computed_at, str):
            raise ValueError("computed_at must be a timezone-aware ISO 8601 string")
        time = datetime.fromisoformat(self.computed_at)
        if time.utcoffset() is None:
            raise ValueError("computed_at must include a timezone")
        object.__setattr__(self, "computed_at", time.astimezone(UTC).isoformat())

    def to_dict(self) -> dict[str, object]:
        return {
            "hash": self.hash,
            "row_count": self.row_count,
            "columns_hashed": list(self.columns_hashed),
            "computed_at": self.computed_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "Fingerprint":
        if not isinstance(data, Mapping) or data.keys() != {
            "hash",
            "row_count",
            "columns_hashed",
            "computed_at",
        }:
            raise ValueError(
                "fingerprint requires hash, row_count, columns_hashed, and computed_at"
            )
        if not isinstance(data["columns_hashed"], list):
            raise ValueError("columns_hashed must be a JSON array")
        return cls(
            data["hash"], data["row_count"], tuple(data["columns_hashed"]), data["computed_at"]
        )


@dataclass(frozen=True)
class FingerprintDecision:
    status: Literal["not_eligible", "no_baseline", "unchanged_suppress", "changed"]
    reason: str


def decide(
    config: FingerprintConfig,
    table_size_bytes: int,
    previous: Fingerprint | None,
    current: Fingerprint | None,
) -> FingerprintDecision:
    if type(table_size_bytes) is not int or table_size_bytes < 0:
        raise ValueError("table_size_bytes must be a nonnegative integer")
    if table_size_bytes > config.size_threshold_bytes:
        return FingerprintDecision(
            "not_eligible", "Table size exceeds threshold; fall back to the normal signal"
        )
    if current is None or current.columns_hashed != config.columns_hashed:
        raise ValueError("an eligible table requires a current fingerprint matching columns_hashed")
    if previous is None or previous.columns_hashed != current.columns_hashed:
        return FingerprintDecision(
            "no_baseline", "No comparable baseline; store current fingerprint and treat as changed"
        )
    if (previous.hash, previous.row_count) == (current.hash, current.row_count):
        return FingerprintDecision(
            "unchanged_suppress",
            "Content fingerprint and row count match; suppress the normal signal",
        )
    return FingerprintDecision("changed", "Content fingerprint or row count differs")


@dataclass(frozen=True)
class KeyDiff:
    added: frozenset[Hashable]
    removed: frozenset[Hashable]
    modified: frozenset[Hashable]


def diff_keys(previous: Mapping[Hashable, int], current: Mapping[Hashable, int]) -> KeyDiff:
    for mapping in (previous, current):
        for value in mapping.values():
            _hash(value)
    before, after = set(previous), set(current)
    return KeyDiff(
        frozenset(after - before),
        frozenset(before - after),
        frozenset(key for key in before & after if previous[key] != current[key]),
    )

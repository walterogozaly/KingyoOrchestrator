"""Validate a narrow delta projection independently of SQL rendering and execution."""

import re
from dataclasses import dataclass

_COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PROJECT = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")


def _identifier(value: object, pattern: re.Pattern[str], field: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{field} must be a supported simple identifier")


def _columns(values: tuple[str, ...], field: str) -> None:
    if not isinstance(values, tuple):
        raise ValueError(f"{field} must be an immutable tuple of column names")
    for value in values:
        _identifier(value, _COLUMN, field)
    if len(values) != len({value.casefold() for value in values}):
        raise ValueError(f"{field} must not contain duplicates (case-insensitive)")


@dataclass(frozen=True)
class DeltaConfig:
    table: str
    key_columns: tuple[str, ...]
    partition_column: str
    declared_columns: tuple[str, ...]
    changed_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.table, str) or len(parts := self.table.split(".")) != 3:
            raise ValueError("table must be an explicit project.dataset.table")
        for part, pattern in zip(parts, (_PROJECT, _COLUMN, _COLUMN), strict=True):
            _identifier(part, pattern, "table component")
        _identifier(self.partition_column, _COLUMN, "partition_column")
        for field in ("key_columns", "declared_columns", "changed_columns"):
            _columns(getattr(self, field), field)
        if not self.key_columns:
            raise ValueError("key_columns must contain at least one key")
        declared = set(self.declared_columns)
        for field, names in (
            ("key_columns", self.key_columns),
            ("partition_column", (self.partition_column,)),
            ("changed_columns", self.changed_columns),
        ):
            missing = set(names) - declared
            if missing:
                raise ValueError(
                    f"{field} contains undeclared columns: {', '.join(sorted(missing))}"
                )

    @property
    def projected_columns(self) -> tuple[str, ...]:
        """Keys in caller order, partition if needed, then other changed columns sorted."""
        columns = list(self.key_columns)
        if self.partition_column not in columns:
            columns.append(self.partition_column)
        columns.extend(name for name in sorted(self.changed_columns) if name not in columns)
        return tuple(columns)

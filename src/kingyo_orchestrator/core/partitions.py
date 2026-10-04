"""Resolve change windows to UTC day selections without accessing data sources."""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

_COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PROJECT = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")


def _identifier(value: object, pattern: re.Pattern[str], field: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{field} must be a supported simple identifier")


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: str

    def __post_init__(self) -> None:
        _identifier(self.name, _COLUMN, "column name")
        if not isinstance(self.data_type, str) or not self.data_type:
            raise ValueError("column data_type must be declared")


@dataclass(frozen=True)
class PartitionConfig:
    table: str
    change_column: str
    partition_column: str
    columns: tuple[ColumnSpec, ...]
    partition_time_zone: str
    granularity: str = "day"

    def __post_init__(self) -> None:
        if not isinstance(self.table, str) or len(parts := self.table.split(".")) != 3:
            raise ValueError("table must be an explicit project.dataset.table")
        for part, pattern in zip(parts, (_PROJECT, _COLUMN, _COLUMN), strict=True):
            _identifier(part, pattern, "table component")
        _identifier(self.change_column, _COLUMN, "change_column")
        _identifier(self.partition_column, _COLUMN, "partition_column")
        if not isinstance(self.partition_time_zone, str) or self.partition_time_zone != "UTC":
            raise ValueError(
                "partition_time_zone must be explicitly declared as 'UTC'; "
                "other partition day boundaries are unsupported (#37)"
            )
        if self.granularity != "day":
            raise ValueError("Only day partition granularity is supported")
        if not isinstance(self.columns, tuple) or not all(
            isinstance(column, ColumnSpec) for column in self.columns
        ):
            raise ValueError("columns must be an immutable tuple of ColumnSpec values")
        names = [column.name.casefold() for column in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("columns must not contain duplicate names (case-insensitive)")
        schema = {column.name: column.data_type for column in self.columns}
        for name in (self.change_column, self.partition_column):
            if name not in schema:
                raise ValueError(f"Missing declared column: {name}")
        if schema[self.change_column] != "TIMESTAMP":
            raise ValueError(f"{self.change_column} must have declared type TIMESTAMP")
        if schema[self.partition_column] not in ("DATE", "TIMESTAMP"):
            raise ValueError(f"{self.partition_column} must have declared type DATE or TIMESTAMP")

    @property
    def partition_type(self) -> str:
        return next(
            column.data_type for column in self.columns if column.name == self.partition_column
        )


@dataclass(frozen=True)
class ChangeWindow:
    since: datetime
    until: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "since", _utc(self.since, "since"))
        object.__setattr__(self, "until", _utc(self.until, "until"))
        if self.until < self.since:
            raise ValueError("until must be greater than or equal to since")

    @property
    def is_empty(self) -> bool:
        return self.since == self.until


@dataclass(frozen=True)
class DayRange:
    """An inclusive range of UTC partition dates."""

    start: date
    end: date


@dataclass(frozen=True)
class PartitionSelection:
    partitions: tuple[date, ...]
    ranges: tuple[DayRange, ...]


class UnsupportedPartitionError(ValueError):
    """Discovery contained a NULL or unsupported partition value."""


def discovery_required(config: PartitionConfig, window: ChangeWindow) -> bool:
    return not window.is_empty and config.change_column != config.partition_column


def _partition_day(value: date | datetime | None) -> date:
    if value is None:
        raise UnsupportedPartitionError("NULL partition values require explicit caller handling")
    if isinstance(value, datetime):
        value = _utc(value, "discovered partition")
        if value.time() != datetime.min.time():
            raise UnsupportedPartitionError(
                "Discovered timestamps must be truncated to UTC midnight"
            )
        return value.date()
    if type(value) is date:
        return value
    raise UnsupportedPartitionError(
        "Discovered partitions must be dates or UTC-midnight timestamps"
    )


def select_partitions(days: Iterable[date]) -> PartitionSelection:
    """Normalize UTC day values and merge adjacent days without bridging gaps."""
    unique: set[date] = set()
    for day in days:
        if type(day) is not date:
            raise UnsupportedPartitionError("Partition days must be Python date values")
        unique.add(day)
    partitions = tuple(sorted(unique))
    ranges: list[DayRange] = []
    for day in partitions:
        if ranges and day.toordinal() == ranges[-1].end.toordinal() + 1:
            ranges[-1] = DayRange(ranges[-1].start, day)
        else:
            ranges.append(DayRange(day, day))
    return PartitionSelection(partitions, tuple(ranges))


def resolve_partitions(
    config: PartitionConfig,
    window: ChangeWindow,
    discovered_partitions: Iterable[date | datetime | None] | None = None,
) -> PartitionSelection:
    """Use caller-supplied discovery results or derive days for identical columns."""
    if window.is_empty:
        return select_partitions(())
    if config.change_column == config.partition_column:
        first = window.since.date().toordinal()
        last = (window.until - timedelta(microseconds=1)).date().toordinal()
        return select_partitions(date.fromordinal(day) for day in range(first, last + 1))
    if discovered_partitions is None:
        raise ValueError(
            "Different columns require discovered_partitions; an empty iterable is valid"
        )
    return select_partitions(_partition_day(value) for value in discovered_partitions)

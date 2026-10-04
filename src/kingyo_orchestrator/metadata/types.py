"""Immutable metadata values with explicit identifiers and JSON-safe encoding."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime


def _require_name(value: object, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")


@dataclass(frozen=True)
class TableRef:
    project: str
    dataset: str
    table: str

    def __post_init__(self) -> None:
        for field in ("project", "dataset", "table"):
            _require_name(getattr(self, field), field)

    def to_dict(self) -> dict[str, str]:
        return {"project": self.project, "dataset": self.dataset, "table": self.table}

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "TableRef":
        if data.keys() != {"project", "dataset", "table"}:
            raise ValueError("TableRef requires exactly project, dataset, and table")
        return cls(project=data["project"], dataset=data["dataset"], table=data["table"])


@dataclass(frozen=True)
class TableSnapshot:
    ref: TableRef
    modified_time: datetime
    num_rows: int
    partitioning_column: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.ref, TableRef):
            raise ValueError("ref must be a TableRef")
        if not isinstance(self.modified_time, datetime) or self.modified_time.utcoffset() is None:
            raise ValueError("modified_time must be a timezone-aware datetime")
        object.__setattr__(self, "modified_time", self.modified_time.astimezone(UTC))
        if type(self.num_rows) is not int or self.num_rows < 0:
            raise ValueError("num_rows must be a nonnegative integer")
        if self.partitioning_column is not None:
            _require_name(self.partitioning_column, "partitioning_column")

    def to_dict(self) -> dict[str, object]:
        return {
            "ref": self.ref.to_dict(),
            "modified_time": self.modified_time.isoformat(),
            "num_rows": self.num_rows,
            "partitioning_column": self.partitioning_column,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "TableSnapshot":
        if data.keys() != {"ref", "modified_time", "num_rows", "partitioning_column"}:
            raise ValueError(
                "TableSnapshot requires ref, modified_time, num_rows, partitioning_column"
            )
        if not isinstance(data["ref"], Mapping):
            raise ValueError("ref must be a mapping")
        if not isinstance(data["modified_time"], str):
            raise ValueError("modified_time must be an ISO 8601 string")
        return cls(
            ref=TableRef.from_dict(data["ref"]),
            modified_time=datetime.fromisoformat(data["modified_time"]),
            num_rows=data["num_rows"],
            partitioning_column=data["partitioning_column"],
        )

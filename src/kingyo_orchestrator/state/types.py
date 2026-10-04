"""Minimal snapshot dictionaries independent of provider and metadata classes."""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TypedDict


class Snapshot(TypedDict):
    modified_time: str
    num_rows: int


SnapshotState = dict[str, Snapshot]


def normalize_snapshot(snapshot: Mapping[str, object]) -> Snapshot:
    """Project the two change fields and canonicalize a timezone-aware timestamp."""
    if not isinstance(snapshot, Mapping):
        raise ValueError("snapshot must be a mapping")
    modified_time = snapshot.get("modified_time")
    if not isinstance(modified_time, str):
        raise ValueError("modified_time must be a timezone-aware ISO 8601 string")
    try:
        timestamp = datetime.fromisoformat(modified_time)
    except ValueError as exc:
        raise ValueError("modified_time must be a timezone-aware ISO 8601 string") from exc
    if timestamp.utcoffset() is None:
        raise ValueError("modified_time must include a timezone")
    num_rows = snapshot.get("num_rows")
    if type(num_rows) is not int or num_rows < 0:
        raise ValueError("num_rows must be a nonnegative integer")
    return {"modified_time": timestamp.astimezone(UTC).isoformat(), "num_rows": num_rows}

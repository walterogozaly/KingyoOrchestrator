"""Versioned JSON checkpoints with atomic replacement and no cloud access."""

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

from .types import SnapshotState, normalize_snapshot


class StateFormatError(ValueError):
    """A checkpoint is unreadable or does not match the supported schema."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON keys are unsupported")
        result[key] = value
    return result


def _normalize_state(snapshots: Mapping[str, Mapping[str, object]]) -> SnapshotState:
    if not isinstance(snapshots, Mapping):
        raise ValueError("snapshots must be a mapping keyed by table id")
    result: SnapshotState = {}
    for table_id, snapshot in snapshots.items():
        if not isinstance(table_id, str) or not table_id.strip():
            raise ValueError("table id must be a nonempty string")
        result[table_id] = normalize_snapshot(snapshot)
    return result


class SnapshotStore:
    """Explicit local file store; each load rereads disk and each save replaces all state."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> SnapshotState:
        """Return empty state for a missing file; reject corrupt state without resetting it."""
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        except UnicodeDecodeError as exc:
            raise StateFormatError("snapshot state must be UTF-8 JSON") from exc
        try:
            data = json.loads(text, object_pairs_hook=_unique_object)
            if not isinstance(data, dict) or data.keys() != {"version", "snapshots"}:
                raise ValueError("state requires exactly version and snapshots")
            if type(data["version"]) is not int or data["version"] != 1:
                raise ValueError("only snapshot state version 1 is supported")
            return _normalize_state(data["snapshots"])
        except ValueError as exc:
            raise StateFormatError(f"invalid snapshot state: {exc}") from exc

    def save(self, snapshots: Mapping[str, Mapping[str, object]]) -> None:
        """Validate before writing, flush a sibling temporary file, then atomically replace."""
        data = {"version": 1, "snapshots": _normalize_state(snapshots)}
        encoded = json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary_path = Path(stream.name)
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, self.path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

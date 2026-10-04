"""Classify observations without reading or writing state."""

from collections.abc import Mapping
from typing import Literal

from kingyo_orchestrator.state.types import normalize_snapshot

ChangeStatus = Literal["first_seen", "changed", "unchanged"]


def compare(previous: Mapping[str, object] | None, current: Mapping[str, object]) -> ChangeStatus:
    """Only modified time and row count determine whether a known table changed."""
    current_snapshot = normalize_snapshot(current)
    if previous is None:
        return "first_seen"
    if normalize_snapshot(previous) != current_snapshot:
        return "changed"
    return "unchanged"

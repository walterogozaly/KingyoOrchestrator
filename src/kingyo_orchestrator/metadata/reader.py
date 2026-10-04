"""Read-only reader contract and a mutable, in-memory test backend."""

from collections.abc import Mapping
from typing import Protocol

from .types import TableRef, TableSnapshot


class TableNotFoundError(LookupError):
    """The explicitly requested table has no observation in the reader."""

    def __init__(self, ref: TableRef) -> None:
        self.ref = ref
        super().__init__(f"Table not found: {ref.project}.{ref.dataset}.{ref.table}")


class MetadataReader(Protocol):
    def get_snapshot(self, ref: TableRef) -> TableSnapshot:
        """Return the current observation, or raise TableNotFoundError."""
        ...


class FakeMetadataReader:
    """Fake observations; construction and reads never access external services."""

    def __init__(self, snapshots: Mapping[TableRef, TableSnapshot]) -> None:
        self._snapshots: dict[TableRef, TableSnapshot] = {}
        for ref, snapshot in snapshots.items():
            if not isinstance(snapshot, TableSnapshot) or ref != snapshot.ref:
                raise ValueError("Each seed key must match its TableSnapshot.ref")
            self.set_snapshot(snapshot)

    def get_snapshot(self, ref: TableRef) -> TableSnapshot:
        try:
            return self._snapshots[ref]
        except KeyError:
            raise TableNotFoundError(ref) from None

    def set_snapshot(self, snapshot: TableSnapshot) -> None:
        """Seed or replace an in-memory observation to simulate new data."""
        if not isinstance(snapshot, TableSnapshot):
            raise ValueError("snapshot must be a TableSnapshot")
        self._snapshots[snapshot.ref] = snapshot

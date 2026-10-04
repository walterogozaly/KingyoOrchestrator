import json
import socket
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from kingyo_orchestrator.metadata import (
    FakeMetadataReader,
    MetadataReader,
    TableNotFoundError,
    TableRef,
    TableSnapshot,
)

REF = TableRef("project_x", "dataset_a", "table_orders")
TIME = datetime(2026, 1, 1, 12, tzinfo=UTC)


@pytest.mark.parametrize("column", [None, "order_sold_ts"])
def test_snapshot_survives_json_round_trip(column):
    snapshot = TableSnapshot(REF, TIME, 42, column)
    encoded = json.loads(json.dumps(snapshot.to_dict()))
    assert encoded == {
        "ref": {"project": "project_x", "dataset": "dataset_a", "table": "table_orders"},
        "modified_time": "2026-01-01T12:00:00+00:00",
        "num_rows": 42,
        "partitioning_column": column,
    }
    assert TableSnapshot.from_dict(encoded) == snapshot
    assert TableRef.from_dict(json.loads(json.dumps(REF.to_dict()))) == REF


def test_equivalent_timezones_have_identical_serialization():
    offset_time = datetime(2026, 1, 1, 7, tzinfo=timezone(timedelta(hours=-5)))
    assert TableSnapshot(REF, offset_time, 0).to_dict() == TableSnapshot(REF, TIME, 0).to_dict()


def test_fake_reads_are_stable_and_explicit_updates_preserve_old_snapshots(monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("metadata fake must stay offline")

    monkeypatch.setattr(socket, "socket", deny_network)
    initial = TableSnapshot(REF, TIME, 42)
    seed = {REF: initial}
    fake = FakeMetadataReader(seed)
    reader: MetadataReader = fake
    seed.clear()
    assert reader.get_snapshot(REF) == initial
    assert reader.get_snapshot(REF) == initial

    updated = replace(initial, modified_time=TIME + timedelta(minutes=1), num_rows=43)
    fake.set_snapshot(updated)
    assert reader.get_snapshot(REF) == updated
    assert initial.num_rows == 42

    other_ref = TableRef("project_x", "dataset_a", "table_customers")
    fake.set_snapshot(TableSnapshot(other_ref, TIME, 3))
    assert reader.get_snapshot(other_ref).num_rows == 3
    assert reader.get_snapshot(REF) == updated


def test_missing_table_raises_typed_error_with_explicit_ref():
    reader = FakeMetadataReader({})
    with pytest.raises(TableNotFoundError, match="project_x.dataset_a.table_orders") as exc:
        reader.get_snapshot(REF)
    assert exc.value.ref == REF


def test_values_are_frozen():
    with pytest.raises(FrozenInstanceError):
        REF.project = "project_y"
    snapshot = TableSnapshot(REF, TIME, 42)
    with pytest.raises(FrozenInstanceError):
        snapshot.num_rows = 43


@pytest.mark.parametrize("field", ["project", "dataset", "table"])
@pytest.mark.parametrize("value", ["", "  ", None, 42])
def test_ref_requires_all_identifiers(field, value):
    data = REF.to_dict()
    data[field] = value
    with pytest.raises(ValueError, match=field):
        TableRef.from_dict(data)


@pytest.mark.parametrize(
    "field, value",
    [
        ("modified_time", datetime(2026, 1, 1)),
        ("modified_time", None),
        ("num_rows", -1),
        ("num_rows", True),
        ("num_rows", 1.5),
        ("partitioning_column", ""),
        ("partitioning_column", 42),
        ("ref", "project_x.dataset_a.table_orders"),
    ],
)
def test_snapshot_rejects_invalid_values(field, value):
    values = {"ref": REF, "modified_time": TIME, "num_rows": 0}
    values[field] = value
    with pytest.raises(ValueError, match=field):
        TableSnapshot(**values)


@pytest.mark.parametrize(
    "update",
    [
        {"modified_time": "2026-01-01T12:00:00"},
        {"modified_time": "invalid"},
        {"modified_time": 42},
        {"ref": "project_x.dataset_a.table_orders"},
        {"num_rows": "42"},
    ],
)
def test_decoding_validates_json_values(update):
    data = TableSnapshot(REF, TIME, 42).to_dict()
    data.update(update)
    with pytest.raises(ValueError):
        TableSnapshot.from_dict(data)


def test_decoding_rejects_missing_or_unknown_fields():
    for data in ({}, {**REF.to_dict(), "location": "US"}):
        with pytest.raises(ValueError, match="exactly"):
            TableRef.from_dict(data)
    data = TableSnapshot(REF, TIME, 42).to_dict()
    del data["num_rows"]
    with pytest.raises(ValueError, match="requires"):
        TableSnapshot.from_dict(data)
    data = TableSnapshot(REF, TIME, 42).to_dict()
    data["unknown"] = None
    with pytest.raises(ValueError, match="requires"):
        TableSnapshot.from_dict(data)


def test_fake_rejects_mismatched_seed_identifiers():
    other_ref = TableRef("project_x", "dataset_a", "table_customers")
    with pytest.raises(ValueError, match="seed key"):
        FakeMetadataReader({other_ref: TableSnapshot(REF, TIME, 42)})
    with pytest.raises(ValueError, match="TableSnapshot"):
        FakeMetadataReader({REF: {}})
    with pytest.raises(ValueError, match="TableSnapshot"):
        FakeMetadataReader({}).set_snapshot({})

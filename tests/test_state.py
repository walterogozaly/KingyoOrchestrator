import json
import socket
from datetime import UTC, datetime
from pathlib import Path

import pytest

from kingyo_orchestrator.core.changes import compare
from kingyo_orchestrator.metadata import FakeMetadataReader, TableRef, TableSnapshot
from kingyo_orchestrator.state import SnapshotStore, StateFormatError

TABLE = "project_x.dataset_a.table_orders"
INITIAL = {"modified_time": "2026-01-01T12:00:00+00:00", "num_rows": 42}
UPDATED = {"modified_time": "2026-01-01T12:01:00+00:00", "num_rows": 43}


def test_comparison_distinguishes_first_seen_from_unchanged():
    assert compare(None, INITIAL) == "first_seen"
    assert compare(INITIAL, dict(INITIAL)) == "unchanged"
    assert compare(None, {**INITIAL, "num_rows": 0}) == "first_seen"


@pytest.mark.parametrize(
    "update",
    [
        {"modified_time": UPDATED["modified_time"]},
        {"num_rows": 43},
        {"num_rows": 0},
        UPDATED,
    ],
)
def test_comparison_changes_for_either_field_in_either_direction(update):
    assert compare(INITIAL, {**INITIAL, **update}) == "changed"
    assert compare({**INITIAL, **update}, INITIAL) == "changed"


def test_comparison_ignores_other_fields_and_equivalent_timestamp_representations():
    assert (
        compare(
            {**INITIAL, "partitioning_column": None},
            {**INITIAL, "partitioning_column": "order_sold_ts"},
        )
        == "unchanged"
    )
    for timestamp in ("2026-01-01T12:00:00Z", "2026-01-01T07:00:00-05:00"):
        assert compare(INITIAL, {**INITIAL, "modified_time": timestamp}) == "unchanged"


def test_missing_file_returns_empty_state_without_creating_anything(tmp_path):
    path = tmp_path / "var" / "snapshots.json"
    assert SnapshotStore(path).load() == {}
    assert not path.parent.exists()


def test_round_trip_uses_versioned_json_and_load_always_rereads_disk(tmp_path):
    path = tmp_path / "var" / "snapshots.json"
    store = SnapshotStore(str(path))
    snapshots = {TABLE: INITIAL, "project_x.dataset_a.table_customers": UPDATED}
    store.save(snapshots)
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "version": 1,
        "snapshots": snapshots,
    }
    loaded = store.load()
    assert loaded == snapshots
    loaded[TABLE]["num_rows"] = 0
    assert store.load() == snapshots
    SnapshotStore(path).save({TABLE: UPDATED})
    assert store.load() == {TABLE: UPDATED}
    store.save({})
    assert store.load() == {}


def test_restart_does_not_duplicate_change_signals_and_stays_offline(tmp_path, monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("snapshot observation must stay offline")

    monkeypatch.setattr(socket, "socket", deny_network)
    ref = TableRef("project_x", "dataset_a", "table_orders")
    reader = FakeMetadataReader({ref: TableSnapshot(ref, datetime(2026, 1, 1, 12, tzinfo=UTC), 42)})
    path = tmp_path / "snapshots.json"
    store = SnapshotStore(path)
    current = reader.get_snapshot(ref).to_dict()
    assert compare(store.load().get(TABLE), current) == "first_seen"
    store.save({TABLE: current})
    assert store.load() == {TABLE: INITIAL}
    assert compare(SnapshotStore(path).load().get(TABLE), current) == "unchanged"

    reader.set_snapshot(TableSnapshot(ref, datetime(2026, 1, 1, 12, 1, tzinfo=UTC), 43))
    current = reader.get_snapshot(ref).to_dict()
    restarted = SnapshotStore(path)
    assert compare(restarted.load().get(TABLE), current) == "changed"
    restarted.save({TABLE: current})
    assert compare(SnapshotStore(path).load().get(TABLE), current) == "unchanged"


def test_save_normalizes_timestamps_without_mutating_input(tmp_path):
    snapshot = {**INITIAL, "modified_time": "2026-01-01T07:00:00-05:00"}
    store = SnapshotStore(tmp_path / "snapshots.json")
    store.save({TABLE: snapshot})
    assert store.load() == {TABLE: INITIAL}
    assert snapshot["modified_time"] == "2026-01-01T07:00:00-05:00"


@pytest.mark.parametrize("has_previous", [False, True])
def test_failed_atomic_replacement_keeps_previous_checkpoint(tmp_path, monkeypatch, has_previous):
    store = SnapshotStore(tmp_path / "snapshots.json")
    if has_previous:
        store.save({TABLE: INITIAL})
    old_bytes = store.path.read_bytes() if has_previous else None

    def interrupted_replace(source, destination):
        assert Path(source).parent == Path(destination).parent
        assert json.loads(Path(source).read_text(encoding="utf-8"))["snapshots"] == {TABLE: UPDATED}
        assert store.load() == ({TABLE: INITIAL} if has_previous else {})
        raise OSError("simulated interruption before replacement")

    monkeypatch.setattr("kingyo_orchestrator.state.store.os.replace", interrupted_replace)
    with pytest.raises(OSError, match="simulated interruption"):
        store.save({TABLE: UPDATED})
    if has_previous:
        assert store.path.read_bytes() == old_bytes
    else:
        assert not store.path.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_failed_flush_preserves_checkpoint_and_cleans_temporary_file(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path / "snapshots.json")
    store.save({TABLE: INITIAL})

    def interrupted_flush(fd):
        raise OSError("simulated disk failure")

    monkeypatch.setattr("kingyo_orchestrator.state.store.os.fsync", interrupted_flush)
    with pytest.raises(OSError, match="simulated disk failure"):
        store.save({TABLE: UPDATED})
    assert store.load() == {TABLE: INITIAL}
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    "text",
    [
        "",
        "{unfinished",
        "[]",
        "null",
        "{}",
        '{"version": 2, "snapshots": {}}',
        '{"version": true, "snapshots": {}}',
        '{"version": 1.0, "snapshots": {}}',
        '{"version": 1, "snapshots": []}',
        '{"version": 1, "snapshots": null}',
        '{"version": 1, "snapshots": {}, "unknown": 0}',
        '{"version": 1, "version": 1, "snapshots": {}}',
        '{"version": 1, "snapshots": {"table_orders": {}, "table_orders": {}}}',
        '{"version": 1, "snapshots": {"table_orders": null}}',
        '{"version": 1, "snapshots": {"table_orders": {}}}',
        json.dumps({"version": 1, "snapshots": {" ": INITIAL}}),
        json.dumps({"version": 1, "snapshots": {TABLE: {**INITIAL, "num_rows": True}}}),
        json.dumps({"version": 1, "snapshots": {TABLE: {**INITIAL, "num_rows": float("nan")}}}),
    ],
)
def test_corrupt_state_is_rejected_without_overwriting_file(tmp_path, text):
    path = tmp_path / "snapshots.json"
    path.write_text(text, encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(StateFormatError, match="invalid snapshot state"):
        SnapshotStore(path).load()
    assert path.read_bytes() == before


def test_invalid_encoding_is_rejected(tmp_path):
    path = tmp_path / "snapshots.json"
    path.write_bytes(b"\xff")
    with pytest.raises(StateFormatError, match="UTF-8"):
        SnapshotStore(path).load()


@pytest.mark.parametrize(
    "snapshot",
    [
        None,
        {},
        {"modified_time": INITIAL["modified_time"]},
        {"num_rows": 42},
        {**INITIAL, "modified_time": 42},
        {**INITIAL, "modified_time": "invalid"},
        {**INITIAL, "modified_time": "2026-01-01T12:00:00"},
        {**INITIAL, "num_rows": True},
        {**INITIAL, "num_rows": -1},
        {**INITIAL, "num_rows": 1.5},
        {**INITIAL, "num_rows": "42"},
    ],
)
def test_invalid_observations_fail_before_replacing_state(tmp_path, snapshot):
    store = SnapshotStore(tmp_path / "snapshots.json")
    store.save({TABLE: INITIAL})
    with pytest.raises(ValueError):
        compare(INITIAL, snapshot)
    if snapshot is not None:
        with pytest.raises(ValueError):
            compare(snapshot, INITIAL)
    with pytest.raises(ValueError):
        store.save({TABLE: snapshot})
    assert store.load() == {TABLE: INITIAL}


@pytest.mark.parametrize("snapshots", [None, [], {"": INITIAL}, {" ": INITIAL}, {42: INITIAL}])
def test_invalid_state_fails_before_creating_parent_directory(tmp_path, snapshots):
    store = SnapshotStore(tmp_path / "var" / "snapshots.json")
    with pytest.raises(ValueError):
        store.save(snapshots)
    assert not store.path.parent.exists()


def test_non_missing_filesystem_errors_propagate(tmp_path):
    with pytest.raises(OSError):
        SnapshotStore(tmp_path).load()

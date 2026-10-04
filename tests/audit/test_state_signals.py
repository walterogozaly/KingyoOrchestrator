"""Offline witnesses and boundary checks from audit #26; no provider calls."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from kingyo_orchestrator.core.changes import compare
from kingyo_orchestrator.metadata import TableRef, TableSnapshot
from kingyo_orchestrator.state import SnapshotStore, StateFormatError

TABLE = "project_x.dataset_a.table_orders"
INITIAL = {"modified_time": "2026-01-01T12:00:00Z", "num_rows": 42}
UPDATED = {"modified_time": "2026-01-01T12:01:00Z", "num_rows": 43}


def canonical(snapshot):
    return {
        **snapshot,
        "modified_time": datetime.fromisoformat(snapshot["modified_time"]).isoformat(),
    }


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="audit #32: precision is truncated")
@pytest.mark.parametrize("persist", [False, True])
def test_distinct_accepted_instants_do_not_disappear(tmp_path, persist):
    before = {**INITIAL, "modified_time": "2026-01-01T12:00:00.0000001Z"}
    after = {**INITIAL, "modified_time": "2026-01-01T12:00:00.0000002Z"}
    try:
        if persist:
            store = SnapshotStore(tmp_path / "state.json")
            store.save({TABLE: before})
            before = store.load()[TABLE]
        status = compare(before, after)
    except ValueError:
        return  # Explicit rejection of unsupported precision is also safe.
    assert status == "changed", status


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="audit #33: key recipe collides")
def test_distinct_refs_do_not_share_baseline(tmp_path):
    try:
        left = TableRef("project_x.dataset_a", "dataset_b", "table_orders")
        right = TableRef("project_x", "dataset_a.dataset_b", "table_orders")
    except ValueError:
        return  # Rejecting ambiguous components is also safe.
    assert left != right

    def key(ref):
        return f"{ref.project}.{ref.dataset}.{ref.table}"

    store = SnapshotStore(tmp_path / "state.json")
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    store.save({key(left): TableSnapshot(left, stamp, 42).to_dict()})
    current = TableSnapshot(right, stamp, 42).to_dict()
    assert compare(store.load().get(key(right)), current) == "first_seen"


@pytest.mark.xfail(strict=True, raises=OverflowError, reason="audit #34: UTC range error escapes")
@pytest.mark.parametrize("stamp", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"])
@pytest.mark.parametrize("entry", ["load", "compare", "metadata"])
def test_out_of_range_utc_uses_documented_validation_error(tmp_path, stamp, entry):
    invalid = {**INITIAL, "modified_time": stamp}
    if entry == "load":
        path = tmp_path / "state.json"
        path.write_text(json.dumps({"version": 1, "snapshots": {TABLE: invalid}}), encoding="utf-8")
        with pytest.raises(StateFormatError):
            SnapshotStore(path).load()
    elif entry == "compare":
        with pytest.raises(ValueError):
            compare(INITIAL, invalid)
    else:
        ref = TableRef("project_x", "dataset_a", "table_orders")
        with pytest.raises(ValueError):
            TableSnapshot(ref, datetime.fromisoformat(stamp), 42)


def test_equal_metadata_cannot_detect_content_replacement():
    before_rows = {"order_a": 10, "order_b": 20}
    after_rows = {"order_a": 10, "order_c": 99}
    assert before_rows != after_rows and len(before_rows) == len(after_rows)
    # The documented classifier sees only metadata, not these row contents.
    assert compare(INITIAL, dict(INITIAL)) == "unchanged"


def test_microsecond_change_and_clock_rollback_are_signals():
    for stamp in ["2026-01-01T12:00:00.000001Z", "2026-01-01T11:59:59Z"]:
        observation = {**INITIAL, "modified_time": stamp}
        assert compare(INITIAL, observation) == "changed"
        assert compare(observation, INITIAL) == "changed"


@pytest.mark.parametrize(
    "stamp",
    ["2026-01-01T12:00:00Z", "2026-01-01 12:00:00+00:00", "2026-01-01T07:00:00-05:00"],
)
def test_supported_equivalent_timestamp_spellings_do_not_repeat_signal(stamp):
    assert compare(INITIAL, {**INITIAL, "modified_time": stamp}) == "unchanged"


@pytest.mark.parametrize("stamp", ["2026-01-01T12:00:00", "2026-01-01T23:59:60Z"])
def test_naive_and_leap_second_like_inputs_fail_closed(stamp):
    with pytest.raises(ValueError):
        compare(INITIAL, {**INITIAL, "modified_time": stamp})


def test_observe_then_crash_before_save_repeats_signal(tmp_path):
    path = tmp_path / "state.json"
    store = SnapshotStore(path)
    store.save({TABLE: INITIAL})
    assert compare(store.load()[TABLE], UPDATED) == "changed"
    # No save: the next instance repeats the event, as documented.
    assert compare(SnapshotStore(path).load()[TABLE], UPDATED) == "changed"


def test_save_before_action_has_no_acknowledgement_protocol(tmp_path):
    path = tmp_path / "state.json"
    store = SnapshotStore(path)
    store.save({TABLE: INITIAL})
    assert compare(store.load()[TABLE], UPDATED) == "changed"
    store.save({TABLE: UPDATED})
    # A caller dying now cannot infer pending work from the observation checkpoint.
    assert compare(SnapshotStore(path).load()[TABLE], UPDATED) == "unchanged"


def test_interleaved_writers_last_save_wins_and_can_erase_a_signal(tmp_path):
    path = tmp_path / "state.json"
    first, second = SnapshotStore(path), SnapshotStore(path)
    first.save({TABLE: INITIAL})
    stale = second.load()
    first.save({TABLE: UPDATED})
    second.save(stale)
    assert first.load()[TABLE] == canonical(INITIAL)
    assert compare(first.load()[TABLE], UPDATED) == "changed"
    # No merge/locking is promised; this demonstrates why the docs require one writer.


def test_hard_exit_before_replace_leaves_ignored_temporary_file(tmp_path):
    path = tmp_path / "state.json"
    store = SnapshotStore(path)
    store.save({TABLE: INITIAL})
    script = """
import os
import sys
from kingyo_orchestrator.state import SnapshotStore
import kingyo_orchestrator.state.store as module
module.os.replace = lambda *args: os._exit(23)
SnapshotStore(sys.argv[1]).save({"project_x.dataset_a.table_orders": {
    "modified_time": "2026-01-01T12:01:00Z", "num_rows": 43
}})
"""
    source = str(Path(__file__).resolve().parents[2] / "src")
    env = {**os.environ, "PYTHONPATH": source}
    run = subprocess.run([sys.executable, "-c", script, str(path)], env=env, check=False)
    assert run.returncode == 23
    assert list(tmp_path.glob(".state.json.*.tmp"))
    assert SnapshotStore(path).load()[TABLE] == canonical(INITIAL)


def test_permission_failure_before_write_preserves_checkpoint(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path / "state.json")
    store.save({TABLE: INITIAL})
    before = store.path.read_bytes()

    def deny_creation(*args, **kwargs):
        raise PermissionError("synthetic directory write denial")

    monkeypatch.setattr(
        "kingyo_orchestrator.state.store.tempfile.NamedTemporaryFile", deny_creation
    )
    with pytest.raises(PermissionError):
        store.save({TABLE: UPDATED})
    assert store.path.read_bytes() == before


@pytest.mark.skipif(sys.platform != "win32", reason="native Windows sharing semantics")
def test_windows_open_destination_without_delete_share_preserves_checkpoint(tmp_path):
    import ctypes
    from ctypes import wintypes

    store = SnapshotStore(tmp_path / "state.json")
    store.save({TABLE: INITIAL})
    before = store.path.read_bytes()
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    # GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE (no DELETE), OPEN_EXISTING.
    handle = kernel.CreateFileW(str(store.path), 0x80000000, 3, None, 3, 0x80, None)
    assert handle != ctypes.c_void_p(-1).value, ctypes.get_last_error()
    try:
        with pytest.raises(PermissionError):
            store.save({TABLE: UPDATED})
        assert store.path.read_bytes() == before
        assert not list(tmp_path.glob(".state.json.*.tmp"))
    finally:
        assert kernel.CloseHandle(handle)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"version":1,"snapshots":',
        b'\xef\xbb\xbf{"version":1,"snapshots":{}}',
        b'{"version":1,"snapshots":{},"version":1}',
        b'{"version":2,"snapshots":{}}',
    ],
)
def test_corruption_is_typed_and_does_not_reset_file(tmp_path, payload):
    path = tmp_path / "state.json"
    path.write_bytes(payload)
    with pytest.raises(StateFormatError):
        SnapshotStore(path).load()
    assert path.read_bytes() == payload


def test_large_checkpoint_round_trip_is_lossless(tmp_path):
    # A bounded 20,000-table probe, not a resource-exhaustion guarantee.
    snapshots = {f"project_x.dataset_a.table_{index}": INITIAL for index in range(20_000)}
    store = SnapshotStore(tmp_path / "state.json")
    store.save(snapshots)
    assert store.load() == {key: canonical(INITIAL) for key in snapshots}


def test_case_and_whitespace_keys_remain_opaque_and_distinct(tmp_path):
    aliases = [TABLE, TABLE.upper(), f" {TABLE} "]
    snapshots = {key: {**INITIAL, "num_rows": index} for index, key in enumerate(aliases)}
    store = SnapshotStore(tmp_path / "state.json")
    store.save(snapshots)
    assert set(store.load()) == set(aliases)
    assert [store.load()[key]["num_rows"] for key in aliases] == [0, 1, 2]
    # Changing a spelling makes it first_seen; normalization is the caller's job.
    assert compare(store.load().get(TABLE.lower() + " "), INITIAL) == "first_seen"


def test_manual_deletion_and_rename_return_first_seen(tmp_path):
    store = SnapshotStore(tmp_path / "state.json")
    store.save({TABLE: INITIAL})
    assert compare(store.load().get("project_x.dataset_a.table_orders_v2"), INITIAL) == "first_seen"
    store.path.unlink()
    assert compare(SnapshotStore(store.path).load().get(TABLE), INITIAL) == "first_seen"

import hashlib
import json
import re
import socket
from dataclasses import FrozenInstanceError, replace

import pytest

from kingyo_orchestrator.adapters.bigquery_fingerprints import (
    render_fingerprint_sql,
    render_key_hashes_sql,
)
from kingyo_orchestrator.fingerprints import (
    Fingerprint,
    FingerprintConfig,
    SnapshotPair,
    apply_audit_policy,
    decide,
    diff_keys,
    propose_audit_columns,
)

TIME = "2026-01-01T12:00:00Z"


def config(**updates):
    values = {
        "table": "project_x.dataset_a.dim_customer",
        "key_columns": ("customer_id",),
        "content_columns": ("name",),
        "audit_columns": ("etl_loaded_at",),
        "size_threshold_bytes": 1024,
    }
    return FingerprintConfig(**{**values, **updates})


def rows(audit="etl_loaded_at", stamp="2026-01-01T12:00:00Z"):
    return {
        "customer_a": {"customer_id": "customer_a", "name": "Alpha", audit: stamp},
        "customer_b": {"customer_id": "customer_b", "name": None, audit: stamp},
    }


def fake_fingerprint(settings, snapshot, computed_at=TIME):
    """Offline fake hashes exercise decisions, not BigQuery's FarmHash algorithm."""
    hashes = {}
    combined = 0
    for key, row in snapshot.items():
        encoded = json.dumps([(name, row[name]) for name in settings.columns_hashed]).encode()
        value = int.from_bytes(hashlib.blake2b(encoded, digest_size=8).digest(), signed=True)
        hashes[key] = value
        combined ^= value
    return Fingerprint(combined, len(snapshot), settings.columns_hashed, computed_at), hashes


def test_audit_only_change_is_suppressed_and_attribute_change_reports_modified_key(monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("fingerprint decisions must stay offline")

    monkeypatch.setattr(socket, "socket", deny_network)
    settings = config()
    previous_rows = rows()
    current_rows = rows(stamp="2026-01-02T12:00:00Z")
    previous, previous_keys = fake_fingerprint(settings, previous_rows)
    current, current_keys = fake_fingerprint(settings, current_rows, "2026-01-02T12:00:00Z")
    assert decide(settings, 100, previous, current).status == "unchanged_suppress"
    assert diff_keys(previous_keys, current_keys).modified == frozenset()
    current_rows["customer_a"]["name"] = "Beta"
    current, current_keys = fake_fingerprint(settings, current_rows)
    assert decide(settings, 100, previous, current).status == "changed"
    assert diff_keys(previous_keys, current_keys).modified == frozenset({"customer_a"})


def test_key_diff_tracks_added_removed_and_composite_keys():
    result = diff_keys(
        {("customer_a", 1): 10, ("customer_b", 2): 20},
        {("customer_a", 1): 11, ("customer_c", 3): 30},
    )
    assert result.added == frozenset({("customer_c", 3)})
    assert result.removed == frozenset({("customer_b", 2)})
    assert result.modified == frozenset({("customer_a", 1)})


def test_size_gate_runs_before_requiring_query_results_and_initial_run_establishes_baseline():
    settings = config()
    decision = decide(settings, 1025, None, None)
    assert decision.status == "not_eligible"
    assert "fall back" in decision.reason
    current, _ = fake_fingerprint(settings, rows())
    assert decide(settings, 1024, None, current).status == "no_baseline"
    assert decide(settings, 0, None, current).status == "no_baseline"
    assert decide(settings, 100, current, replace(current, row_count=3)).status == "changed"
    assert (
        decide(settings, 100, replace(current, columns_hashed=("customer_id",)), current).status
        == "no_baseline"
    )
    with pytest.raises(ValueError, match="current fingerprint"):
        decide(settings, 100, None, None)


def test_fingerprint_json_round_trip_and_frozen_records():
    current, _ = fake_fingerprint(config(), rows())
    data = json.loads(json.dumps(current.to_dict()))
    assert data == {
        "hash": current.hash,
        "row_count": 2,
        "columns_hashed": ["customer_id", "name"],
        "computed_at": "2026-01-01T12:00:00+00:00",
    }
    assert Fingerprint.from_dict(data) == current
    assert replace(current, computed_at="2026-01-01T07:00:00-05:00") == current
    with pytest.raises(FrozenInstanceError):
        current.hash = 0
    with pytest.raises(FrozenInstanceError):
        config().table = "project_x.dataset_a.dim_other"


def test_sql_reads_only_configured_keys_and_content_and_is_order_independent():
    settings = config(content_columns=("name", "valid_date"))
    predicate = "  `valid_date` IN (DATE '2026-01-01', DATE '2026-01-03')  "
    for renderer in (render_fingerprint_sql, render_key_hashes_sql):
        sql = renderer(settings, predicate)
        assert (
            "FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(`customer_id`, `name`, `valid_date`)))" in sql
        )
        assert "etl_loaded_at" not in sql
        assert "*" not in sql
        assert set(re.findall(r"`([^`]+)`", sql)) == {
            settings.table,
            "customer_id",
            "name",
            "valid_date",
        }
        assert sql == renderer(
            replace(settings, content_columns=tuple(reversed(settings.content_columns))), predicate
        )
        assert "\nWHERE " + predicate in sql
    aggregate = render_fingerprint_sql(settings)
    assert "COALESCE(BIT_XOR(row_hash), 0)" in aggregate
    assert "COUNT(1) AS row_count" in aggregate
    assert render_key_hashes_sql(settings).startswith("SELECT `customer_id`, FARM_FINGERPRINT")


def test_two_confirmed_audit_columns_and_arbitrary_names_are_excluded():
    settings = config(audit_columns=("run_stamp", "ingested_time"))
    before = {
        key: {**row, "run_stamp": "first", "ingested_time": "first"} for key, row in rows().items()
    }
    after = {
        key: {**row, "run_stamp": "second", "ingested_time": "second"}
        for key, row in before.items()
    }
    first, _ = fake_fingerprint(settings, before)
    second, _ = fake_fingerprint(settings, after)
    assert decide(settings, 100, first, second).status == "unchanged_suppress"
    sql = render_fingerprint_sql(settings)
    assert all(name not in sql for name in settings.audit_columns)
    assert apply_audit_policy(settings, (), {}, auto_apply=True) is settings


@pytest.mark.parametrize("audit", ["last_upd_ts", "etl_loaded_at", "run_clock"])
@pytest.mark.parametrize("data_type", ["TIMESTAMP", "DATETIME"])
def test_candidates_are_name_independent_and_high_confidence_needs_other_columns_unchanged(
    audit, data_type
):
    types = {"customer_id": "STRING", "name": "STRING", audit: data_type}
    before, after = rows(audit), rows(audit, "2026-01-02T12:00:00Z")
    (candidate,) = propose_audit_columns(before, after, types)
    assert (candidate.column, candidate.confidence) == (audit, "high")
    assert "every matched row" in candidate.reason
    after["customer_a"]["name"] = "Beta"
    assert propose_audit_columns(before, after, types) == ()


def test_partial_attribute_timestamps_and_non_timestamp_columns_are_not_proposed():
    before = rows("ship_ts")
    after = rows("ship_ts")
    after["customer_a"]["ship_ts"] = "2026-01-02T12:00:00Z"
    types = {"customer_id": "STRING", "name": "STRING", "ship_ts": "TIMESTAMP"}
    assert propose_audit_columns(before, after, types) == ()
    after = rows("ship_ts", "2026-01-02T12:00:00Z")
    assert propose_audit_columns(before, after, {**types, "ship_ts": "STRING"}) == ()


def test_constant_value_is_only_low_confidence_and_never_auto_applied():
    values = rows()
    types = {"customer_id": "STRING", "name": "STRING", "etl_loaded_at": "TIMESTAMP"}
    (candidate,) = propose_audit_columns(values, values, types)
    assert candidate.confidence == "low"
    settings = config(content_columns=("name", "etl_loaded_at"), audit_columns=())
    pairs = (SnapshotPair(values, values), SnapshotPair(values, values))
    assert apply_audit_policy(settings, pairs, types, auto_apply=True) is settings


def test_auto_exclusion_is_opt_in_and_requires_two_consecutive_high_confidence_pairs():
    before, middle, after = (
        rows(),
        rows(stamp="2026-01-02T12:00:00Z"),
        rows(stamp="2026-01-03T12:00:00Z"),
    )
    types = {"customer_id": "STRING", "name": "STRING", "etl_loaded_at": "TIMESTAMP"}
    settings = config(content_columns=("name", "etl_loaded_at"), audit_columns=())
    pairs = (SnapshotPair(before, middle), SnapshotPair(middle, after))
    assert apply_audit_policy(settings, pairs, types) is settings
    assert apply_audit_policy(settings, pairs[:1], types, auto_apply=True) is settings
    applied = apply_audit_policy(settings, pairs, types, auto_apply=True)
    assert applied.audit_columns == ("etl_loaded_at",)
    assert applied.content_columns == ("name",)
    first, _ = fake_fingerprint(applied, before)
    last, _ = fake_fingerprint(applied, after)
    assert decide(applied, 100, first, last).status == "unchanged_suppress"
    assert "etl_loaded_at" not in render_fingerprint_sql(applied)
    with pytest.raises(ValueError, match="consecutive"):
        apply_audit_policy(
            settings,
            (SnapshotPair(before, middle), SnapshotPair(before, after)),
            types,
            auto_apply=True,
        )


def test_detection_requires_complete_stable_row_sets_and_non_null_timestamps():
    before, after = rows(), rows(stamp="2026-01-02T12:00:00Z")
    types = {"customer_id": "STRING", "name": "STRING", "etl_loaded_at": "TIMESTAMP"}
    assert propose_audit_columns({}, {}, types) == ()
    assert (
        propose_audit_columns(
            {"customer_a": before["customer_a"]}, {"customer_a": after["customer_a"]}, types
        )
        == ()
    )
    assert propose_audit_columns(before, {"customer_a": after["customer_a"]}, types) == ()
    after["customer_a"]["etl_loaded_at"] = None
    assert propose_audit_columns(before, after, types) == ()
    del after["customer_a"]["name"]
    with pytest.raises(ValueError, match="exactly the declared"):
        propose_audit_columns(before, after, types)


@pytest.mark.parametrize(
    "predicate",
    [
        "",
        "valid_date IN (SELECT valid_date FROM table_x)",
        "DATE(valid_date) IN (DATE '2026-01-01')",
        "valid_date IN (CURRENT_DATE())",
        "valid_date IN (DATE '2026-02-30')",
        "etl_loaded_at IN (DATE '2026-01-01')",
        "unknown_date IN (DATE '2026-01-01')",
        42,
    ],
)
def test_filters_reject_subqueries_expressions_audit_unknown_and_invalid_dates(predicate):
    with pytest.raises(ValueError):
        render_fingerprint_sql(config(content_columns=("name", "valid_date")), predicate)


@pytest.mark.parametrize(
    "update",
    [
        {"key_columns": ()},
        {"key_columns": ("row_hash",)},
        {"content_columns": ("name", "NAME")},
        {"audit_columns": ("name",)},
        {"audit_columns": ("customer_id",)},
        {"content_columns": ("customer_id",)},
        {"key_columns": ["customer_id"]},
        {"audit_columns": ("bad.column",)},
        {"table": "dim_customer"},
        {"size_threshold_bytes": 0},
        {"size_threshold_bytes": True},
    ],
)
def test_invalid_configs_are_rejected(update):
    with pytest.raises(ValueError):
        config(**update)


@pytest.mark.parametrize(
    "update",
    [
        {"hash": True},
        {"hash": 2**63},
        {"row_count": -1},
        {"row_count": True},
        {"columns_hashed": ()},
        {"computed_at": "2026-01-01"},
        {"computed_at": "invalid"},
    ],
)
def test_invalid_fingerprint_records_are_rejected(update):
    values = {
        "hash": 10,
        "row_count": 2,
        "columns_hashed": ("customer_id", "name"),
        "computed_at": TIME,
    }
    with pytest.raises(ValueError):
        Fingerprint(**{**values, **update})

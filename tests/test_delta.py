import re
import socket
from dataclasses import FrozenInstanceError, replace

import pytest

from kingyo_orchestrator.adapters.bigquery_delta import render_delta_select
from kingyo_orchestrator.core.delta import DeltaConfig


def config(**updates):
    values = {
        "table": "project_x.dataset_a.table_orders",
        "key_columns": ("order_id",),
        "partition_column": "partition_date",
        "declared_columns": ("order_id", "partition_date", "amount", "status", "notes"),
        "changed_columns": ("status", "amount"),
    }
    return DeltaConfig(**{**values, **updates})


def select_list(sql):
    projection = sql.split("\nFROM ", 1)[0].removeprefix("SELECT\n  ")
    return tuple(re.findall(r"`([^`]+)`", projection))


def test_fifty_column_table_projects_exactly_six_columns_and_stays_offline(monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("delta rendering must stay offline")

    monkeypatch.setattr(socket, "socket", deny_network)
    columns = ("order_id", "partition_date", *(f"col_{i:02d}" for i in range(48)))
    changed = ("col_12", "col_02", "col_09", "col_05")
    sql = render_delta_select(config(declared_columns=columns, changed_columns=changed))
    projected = select_list(sql)
    assert len(columns) == 50
    assert projected == ("order_id", "partition_date", "col_02", "col_05", "col_09", "col_12")
    assert len(projected) == 6
    assert "*" not in sql
    assert all(name not in projected for name in set(columns) - set(projected))


def test_generated_select_has_exact_quoted_projection_and_table():
    assert render_delta_select(config()) == (
        "SELECT\n  `order_id`,\n  `partition_date`,\n  `amount`,\n  `status`\n"
        "FROM `project_x.dataset_a.table_orders`"
    )


def test_composite_key_order_is_preserved_and_changes_are_sorted():
    values = config(
        key_columns=("status", "order_id"),
        changed_columns=("notes", "amount", "order_id", "partition_date", "status"),
    )
    expected = ("status", "order_id", "partition_date", "amount", "notes")
    assert values.projected_columns == expected
    assert select_list(render_delta_select(values)) == expected
    assert render_delta_select(
        replace(values, changed_columns=tuple(reversed(values.changed_columns)))
    ) == render_delta_select(values)
    assert render_delta_select(
        replace(values, declared_columns=tuple(reversed(values.declared_columns)))
    ) == render_delta_select(values)


def test_partition_key_and_changed_keys_are_projected_only_once():
    values = config(
        key_columns=("partition_date", "order_id"),
        changed_columns=("partition_date", "order_id", "amount"),
    )
    assert select_list(render_delta_select(values)) == ("partition_date", "order_id", "amount")


def test_empty_changes_mean_keys_and_partition_only():
    assert select_list(render_delta_select(config(changed_columns=()))) == (
        "order_id",
        "partition_date",
    )
    values = config(key_columns=("partition_date",), changed_columns=())
    assert select_list(render_delta_select(values)) == ("partition_date",)


def test_predicate_is_preserved_verbatim_and_optional():
    predicate = "  `partition_date` IN (\n  DATE '2026-01-01', DATE '2026-01-03'\n)  "
    base = render_delta_select(config())
    assert render_delta_select(config(), predicate) == base + "\nWHERE " + predicate
    assert "WHERE" not in render_delta_select(config(), None)
    assert select_list(render_delta_select(config(), predicate)) == select_list(base)


@pytest.mark.parametrize("predicate", ["", "  ", "\n", 42, False])
def test_blank_or_non_string_predicates_are_rejected(predicate):
    with pytest.raises(ValueError, match="predicate must be a nonblank string or None"):
        render_delta_select(config(), predicate)


@pytest.mark.parametrize(
    "update, message",
    [
        ({"key_columns": ()}, "at least one key"),
        ({"key_columns": ("unknown_id",)}, "key_columns contains undeclared"),
        ({"partition_column": "unknown_partition"}, "partition_column contains undeclared"),
        ({"changed_columns": ("unknown_value",)}, "changed_columns contains undeclared"),
        ({"declared_columns": ()}, "undeclared"),
        ({"key_columns": ("order_id", "order_id")}, "key_columns.*duplicates"),
        ({"key_columns": ("order_id", "ORDER_ID")}, "key_columns.*duplicates"),
        ({"changed_columns": ("amount", "amount")}, "changed_columns.*duplicates"),
        ({"changed_columns": ("amount", "AMOUNT")}, "changed_columns.*duplicates"),
        ({"declared_columns": ("order_id", "ORDER_ID")}, "declared_columns.*duplicates"),
        ({"key_columns": ["order_id"]}, "immutable tuple"),
        ({"declared_columns": ["order_id", "partition_date"]}, "immutable tuple"),
        ({"changed_columns": {"amount"}}, "immutable tuple"),
        ({"changed_columns": "amount"}, "immutable tuple"),
    ],
)
def test_invalid_schema_or_mutable_collections_are_rejected(update, message):
    with pytest.raises(ValueError, match=message):
        config(**update)


@pytest.mark.parametrize(
    "name", ["", "with space", "nested.field", "*", "`amount`", "amount;DROP", "amount\n", 42]
)
@pytest.mark.parametrize(
    "field", ["key_columns", "partition_column", "declared_columns", "changed_columns"]
)
def test_unsupported_column_identifiers_are_rejected(name, field):
    value = name if field == "partition_column" else (name,)
    with pytest.raises(ValueError, match="simple identifier"):
        config(**{field: value})


@pytest.mark.parametrize(
    "table",
    [
        "table_orders",
        "dataset_a.table_orders",
        "project_x.dataset_a.table_orders.extra",
        "project_x.dataset_a.*",
        "project_x.dataset-a.table_orders",
        "project_x.dataset_a.table orders",
        "project_x.dataset_a.`table_orders`",
        None,
    ],
)
def test_table_requires_three_explicit_simple_components(table):
    with pytest.raises(ValueError, match="explicit project.dataset.table|simple identifier"):
        config(table=table)


def test_supported_identifiers_match_partition_config_style_and_are_quoted():
    values = config(
        table="project-x.dataset_a.table_orders",
        key_columns=("select",),
        partition_column="_partition_date",
        declared_columns=("select", "_partition_date", "_value1"),
        changed_columns=("_value1",),
    )
    sql = render_delta_select(values)
    assert select_list(sql) == ("select", "_partition_date", "_value1")
    assert "FROM `project-x.dataset_a.table_orders`" in sql


def test_inputs_are_frozen_and_default_changes_are_empty():
    values = config()
    with pytest.raises(FrozenInstanceError):
        values.table = "project_x.dataset_a.table_other"
    with pytest.raises(TypeError):
        values.key_columns[0] = "other_id"
    values = DeltaConfig(
        "project_x.dataset_a.table_orders",
        ("order_id",),
        "partition_date",
        ("order_id", "partition_date"),
    )
    assert values.changed_columns == ()
    assert select_list(render_delta_select(values)) == ("order_id", "partition_date")

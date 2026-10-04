"""Render discovery SQL strings; this module never submits or executes queries."""

from kingyo_orchestrator.core.partitions import ChangeWindow, PartitionConfig


def render_discovery_sql(config: PartitionConfig, window: ChangeWindow) -> str:
    """Select distinct UTC-midnight values, preserving NULL for explicit handling."""
    since = window.since.isoformat(sep=" ", timespec="microseconds")
    until = window.until.isoformat(sep=" ", timespec="microseconds")
    return (
        f"SELECT DISTINCT TIMESTAMP_TRUNC(`{config.partition_column}`, DAY, 'UTC') "
        "AS partition_start\n"
        f"FROM `{config.table}`\n"
        f"WHERE `{config.change_column}` >= TIMESTAMP '{since}'\n"
        f"  AND `{config.change_column}` < TIMESTAMP '{until}'"
    )

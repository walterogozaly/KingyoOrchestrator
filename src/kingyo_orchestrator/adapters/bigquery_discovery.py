"""Render discovery SQL strings; this module never submits or executes queries."""

from kingyo_orchestrator.core.partitions import ChangeWindow, PartitionConfig


def render_discovery_sql(config: PartitionConfig, window: ChangeWindow) -> str:
    """Select distinct DATE or UTC-midnight values, preserving NULL for handling."""
    since = window.since.isoformat(sep=" ", timespec="microseconds")
    until = window.until.isoformat(sep=" ", timespec="microseconds")
    column = f"`{config.partition_column}`"
    expression = (
        column if config.partition_type == "DATE" else f"TIMESTAMP_TRUNC({column}, DAY, 'UTC')"
    )
    return (
        f"SELECT DISTINCT {expression} AS partition_start\n"
        f"FROM `{config.table}`\n"
        f"WHERE `{config.change_column}` >= TIMESTAMP '{since}'\n"
        f"  AND `{config.change_column}` < TIMESTAMP '{until}'"
    )

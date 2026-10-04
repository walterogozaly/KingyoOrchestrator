"""Render constant predicates on raw partition columns, without cloud access."""

import re
from datetime import date, timedelta

from kingyo_orchestrator.core.partitions import (
    PartitionConfig,
    PartitionSelection,
    select_partitions,
)

MAX_PARTITION_DAYS = 1000


def render_partition_filter(
    config: PartitionConfig,
    selection: PartitionSelection,
    alias: str | None = None,
) -> str:
    """Render literal DATE IN-lists or half-open UTC TIMESTAMP ranges."""
    if alias is not None and (
        not isinstance(alias, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias) is None
    ):
        raise ValueError("alias must be a supported simple identifier")
    column = f"`{config.partition_column}`"
    if alias is not None:
        column = f"`{alias}`.{column}"

    # Derive ranges from the actual days, never trust stale caller-supplied ranges.
    normalized = select_partitions(selection.partitions)
    if len(normalized.partitions) > MAX_PARTITION_DAYS:
        raise ValueError(f"Partition filters support at most {MAX_PARTITION_DAYS} unique days")
    if not normalized.partitions:
        return "FALSE"
    if config.partition_type == "DATE":
        literals = ",\n  ".join(f"DATE '{day.isoformat()}'" for day in normalized.partitions)
        return f"{column} IN (\n  {literals}\n)"

    predicates = []
    for days in normalized.ranges:
        if days.end == date.max:
            raise ValueError(
                "TIMESTAMP filter cannot represent an exclusive bound after 9999-12-31"
            )
        end = days.end + timedelta(days=1)
        predicates.append(
            f"({column} >= TIMESTAMP '{days.start.isoformat()} 00:00:00+00:00' "
            f"AND {column} < TIMESTAMP '{end.isoformat()} 00:00:00+00:00')"
        )
    return predicates[0] if len(predicates) == 1 else "(" + " OR ".join(predicates) + ")"

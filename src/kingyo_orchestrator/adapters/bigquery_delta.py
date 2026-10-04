"""Render a BigQuery delta SELECT as text without creating or querying any table."""

from kingyo_orchestrator.core.delta import DeltaConfig


def render_delta_select(config: DeltaConfig, predicate: str | None = None) -> str:
    """Project only mandatory and changed columns; preserve a supplied predicate verbatim."""
    if predicate is not None and (not isinstance(predicate, str) or not predicate.strip()):
        raise ValueError("predicate must be a nonblank string or None")
    projection = ",\n  ".join(f"`{name}`" for name in config.projected_columns)
    sql = f"SELECT\n  {projection}\nFROM `{config.table}`"
    if predicate is not None:
        sql += f"\nWHERE {predicate}"
    return sql

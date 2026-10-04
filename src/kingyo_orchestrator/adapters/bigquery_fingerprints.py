"""Fingerprint SQL strings only; no metadata access, submission, or execution."""

import re
from datetime import date

from kingyo_orchestrator.fingerprints import FingerprintConfig

_FILTER = re.compile(
    r"\s*(?P<column>`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)\s+IN\s*"
    r"\(\s*DATE\s+'\d{4}-\d{2}-\d{2}'(?:\s*,\s*DATE\s+'\d{4}-\d{2}-\d{2}')*\s*\)\s*",
    re.IGNORECASE,
)


def _where(config: FingerprintConfig, partition_filter: str | None) -> str:
    if partition_filter is None:
        return ""
    match = _FILTER.fullmatch(partition_filter) if isinstance(partition_filter, str) else None
    if match is None:
        raise ValueError("partition_filter must be a simple column IN-list of literal DATE values")
    if match["column"].strip("`") not in config.columns_hashed:
        raise ValueError("partition filter column must be a key or content column, never audit")
    for literal in re.findall(r"'([^']+)'", partition_filter):
        date.fromisoformat(literal)
    return f"\nWHERE {partition_filter}"


def _row_hash(config: FingerprintConfig) -> str:
    columns = ", ".join(f"`{name}`" for name in config.columns_hashed)
    return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({columns}))) AS row_hash"


def render_key_hashes_sql(config: FingerprintConfig, partition_filter: str | None = None) -> str:
    """One key/hash row per input row; the caller must guarantee unique, non-NULL keys."""
    keys = ", ".join(f"`{name}`" for name in config.key_columns)
    return f"SELECT {keys}, {_row_hash(config)}\nFROM `{config.table}`" + _where(
        config, partition_filter
    )


def render_fingerprint_sql(config: FingerprintConfig, partition_filter: str | None = None) -> str:
    """COUNT(1) counts all rows without introducing a wildcard into rendered text."""
    inner = f"SELECT {_row_hash(config)}\nFROM `{config.table}`" + _where(config, partition_filter)
    return (
        "SELECT COALESCE(BIT_XOR(row_hash), 0) AS content_hash, COUNT(1) AS row_count\nFROM (\n"
        + inner
        + "\n)"
    )

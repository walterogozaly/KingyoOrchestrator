"""Minimal SQLX reader: config block + SQL body + ref() dependencies.

Deliberately not a JS evaluator. Supports `type`, `name`, and `bigquery.partitionBy`
(a SQL expression over the model's own output columns, e.g. "DATE(last_upd_ts)").
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

REF = re.compile(r"\$\{\s*ref\(\s*(?:\"[^\"]*\"\s*,\s*)?\"([^\"]+)\"\s*\)\s*\}")


@dataclass
class Model:
    name: str
    type: str  # table | view | declaration
    sql: str  # body with ${ref()} replaced by bare table names
    deps: list[str] = field(default_factory=list)
    partition_expr: str | None = None  # SQL expr over this model's output columns
    unique_key: str | None = None  # declared on mutable (upsert) sources; absent => append-only


def _split_config(text: str) -> tuple[str, str]:
    m = re.match(r"\s*config\s*\{", text)
    if not m:
        return "", text
    depth, i = 1, m.end()
    while depth:
        depth += {"{": 1, "}": -1}.get(text[i], 0)
        i += 1
    return text[m.end() : i - 1], text[i:]


def parse_sqlx(path: Path) -> Model:
    cfg, body = _split_config(path.read_text())
    type_ = (re.search(r"type:\s*\"(\w+)\"", cfg) or [None, "table"])[1]
    name_m = re.search(r"\bname:\s*\"(\w+)\"", cfg)
    part_m = re.search(r"partitionBy:\s*\"([^\"]+)\"", cfg)
    key_m = re.search(r"uniqueKey:\s*\"(\w+)\"", cfg)
    deps = REF.findall(body)
    return Model(
        name=name_m[1] if name_m else path.stem,
        type=type_,
        sql=REF.sub(r"\1", body).strip().rstrip(";"),
        deps=list(dict.fromkeys(deps)),
        partition_expr=part_m[1] if part_m else None,
        unique_key=key_m[1] if key_m else None,
    )


def load_repo(root: str | Path) -> dict[str, Model]:
    models = {}
    for p in sorted(Path(root).rglob("*.sqlx")):
        m = parse_sqlx(p)
        models[m.name] = m
    return models

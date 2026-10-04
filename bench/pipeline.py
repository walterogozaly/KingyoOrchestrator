"""Minimal Dataform-style pipeline loader and baseline builder, independent of any orchestrator.

Reads `config { ... }` + SQL bodies, resolves `${ref("x")}`, and rebuilds every non-declaration model in
topological order. This is the "plain Dataform" baseline the benchmark compares candidates against.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from pathlib import Path

REF = re.compile(r"\$\{\s*ref\(\s*\"([^\"]+)\"\s*\)\s*\}")


@dataclass
class Model:
    name: str
    type: str
    sql: str
    deps: list[str] = field(default_factory=list)
    partition_expr: str | None = None


def load_repo(root: str | Path) -> dict[str, Model]:
    models = {}
    for p in sorted(Path(root).rglob("*.sqlx")):
        text = p.read_text()
        cfg, body = "", text
        m = re.match(r"\s*config\s*\{", text)
        if m:
            depth, i = 1, m.end()
            while depth:
                depth += {"{": 1, "}": -1}.get(text[i], 0)
                i += 1
            cfg, body = text[m.end() : i - 1], text[i:]
        kind = (re.search(r'type:\s*"(\w+)"', cfg) or [None, "table"])[1]
        name = (re.search(r'\bname:\s*"(\w+)"', cfg) or [None, p.stem])[1]
        part = re.search(r'partitionBy:\s*"([^"]+)"', cfg)
        models[name] = Model(
            name,
            kind,
            REF.sub(r"\1", body).strip(),
            list(dict.fromkeys(REF.findall(body))),
            part[1] if part else None,
        )
    return models


def topo_order(models: dict[str, Model]) -> list[str]:
    return list(TopologicalSorter({n: m.deps for n, m in models.items()}).static_order())


def build_all(con, models: dict[str, Model]) -> None:
    for n in topo_order(models):
        if models[n].type != "declaration":
            con.execute(f"CREATE OR REPLACE TABLE {n} AS {models[n].sql}")

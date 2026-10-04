from __future__ import annotations

from graphlib import TopologicalSorter

from .sqlx import Model


class Dag:
    def __init__(self, models: dict[str, Model]):
        self.models = models
        for m in models.values():
            for d in m.deps:
                if d not in models:
                    raise KeyError(f"{m.name} refs unknown table {d}")
        self.order = list(TopologicalSorter({n: m.deps for n, m in models.items()}).static_order())

    def children(self, name: str) -> list[str]:
        return [n for n in self.order if name in self.models[n].deps]

    def downstream(self, names) -> list[str]:
        """Models reachable from `names` (excluding them), in topological order."""
        seen, stack = set(), list(names)
        while stack:
            for c in self.children(stack.pop()):
                if c not in seen:
                    seen.add(c)
                    stack.append(c)
        return [n for n in self.order if n in seen]

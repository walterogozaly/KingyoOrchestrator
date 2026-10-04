"""Deterministic downstream impact analysis over a minimal string-id graph."""

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from heapq import heapify, heappop, heappush
from typing import Protocol


class Graph(Protocol):
    def nodes(self) -> Iterable[str]: ...

    def parents(self, node: str) -> Iterable[str]: ...

    def children(self, node: str) -> Iterable[str]: ...


class GraphValidationError(ValueError):
    """Graph ids or adjacency do not satisfy the supported graph contract."""


class GraphCycleError(GraphValidationError):
    """A cycle prevents topological ordering; unresolved nodes may include descendants."""

    def __init__(self, unresolved_nodes: tuple[str, ...]) -> None:
        self.unresolved_nodes = unresolved_nodes
        super().__init__(f"graph contains a cycle; unresolved nodes: {', '.join(unresolved_nodes)}")


@dataclass(frozen=True)
class PlanStep:
    action: str
    reason: str
    triggered_by: tuple[str, ...]


@dataclass(frozen=True)
class Plan:
    steps: tuple[PlanStep, ...]
    unknown_sources: tuple[str, ...]


def _ids(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, str):
        raise GraphValidationError("ids must be an iterable of strings, not a single string")
    ids = tuple(values)
    if any(not isinstance(value, str) or not value.strip() for value in ids):
        raise GraphValidationError("node and source ids must be nonempty strings")
    return ids


def _read_graph(graph: Graph) -> tuple[set[str], dict[str, set[str]], dict[str, set[str]]]:
    ids = _ids(graph.nodes())
    nodes = set(ids)
    if len(nodes) != len(ids):
        raise GraphValidationError("graph.nodes() must not contain duplicate ids")
    parents = {node: set(_ids(graph.parents(node))) for node in sorted(nodes)}
    children = {node: set(_ids(graph.children(node))) for node in sorted(nodes)}
    for node in sorted(nodes):
        missing = (parents[node] | children[node]) - nodes
        if missing:
            raise GraphValidationError(
                f"adjacency for {node} references missing nodes: {', '.join(sorted(missing))}"
            )
    for node in sorted(nodes):
        for child in sorted(children[node]):
            if node not in parents[child]:
                raise GraphValidationError(f"parents/children disagree on edge {node} -> {child}")
        for parent in sorted(parents[node]):
            if node not in children[parent]:
                raise GraphValidationError(f"parents/children disagree on edge {parent} -> {node}")
    return nodes, parents, children


def _topological_order(
    nodes: set[str], parents: dict[str, set[str]], children: dict[str, set[str]]
) -> tuple[str, ...]:
    indegree = {node: len(parents[node] & nodes) for node in nodes}
    ready = [node for node, degree in indegree.items() if degree == 0]
    heapify(ready)
    ordered = []
    while ready:
        node = heappop(ready)
        ordered.append(node)
        for child in sorted(children[node] & nodes):
            indegree[child] -= 1
            if indegree[child] == 0:
                heappush(ready, child)
    if len(ordered) != len(nodes):
        raise GraphCycleError(tuple(sorted(node for node, degree in indegree.items() if degree)))
    return tuple(ordered)


def plan(graph: Graph, changed_sources: Iterable[str]) -> Plan:
    """Plan every proper descendant, with one shortest reason and all triggering sources."""
    nodes, parents, children = _read_graph(graph)
    _topological_order(nodes, parents, children)  # reject cycles even in untouched branches
    changed = set(_ids(changed_sources))
    unknown = tuple(sorted(changed - nodes))
    triggers: dict[str, list[str]] = {}
    reasons: dict[str, tuple[str, ...]] = {}
    for source in sorted(changed & nodes):
        paths = {source: (source,)}
        pending = deque([source])
        while pending:
            parent = pending.popleft()
            for child in sorted(children[parent]):
                if child in paths:
                    continue
                path = (*paths[parent], child)
                paths[child] = path
                pending.append(child)
                triggers.setdefault(child, []).append(source)
                previous = reasons.get(child)
                if previous is None or (len(path), path) < (len(previous), previous):
                    reasons[child] = path
    steps = []
    for action in _topological_order(set(triggers), parents, children):
        path = reasons[action]
        steps.append(
            PlanStep(
                action=action,
                reason=f"Changed source {path[0]} via {' -> '.join(path)}",
                triggered_by=tuple(triggers[action]),
            )
        )
    return Plan(steps=tuple(steps), unknown_sources=unknown)

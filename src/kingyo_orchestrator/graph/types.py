"""Provider-independent, immutable action graphs and deterministic traversal."""

import re
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType


class CompiledGraphError(ValueError):
    """The supplied compiled graph uses a malformed or unsupported shape."""


class UnknownActionError(KeyError):
    """A requested action is not a graph node."""


@dataclass(frozen=True, order=True)
class ActionId:
    database: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        for field_name in ("database", "schema", "name"):
            value = getattr(self, field_name)
            pattern = (
                r"[A-Za-z_][A-Za-z0-9_-]*"
                if field_name == "database"
                else r"[A-Za-z_][A-Za-z0-9_]*"
            )
            if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
                raise CompiledGraphError(
                    f"target.{field_name} must be a supported simple identifier"
                )

    def __str__(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"


@dataclass(frozen=True)
class Action:
    id: ActionId
    type: str
    dependencies: tuple[ActionId, ...] = ()
    query: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, ActionId):
            raise CompiledGraphError("Action.id must be an ActionId")
        if self.type not in ("table", "view", "incremental"):
            raise CompiledGraphError("Action.type must be table, view, or incremental")
        if not isinstance(self.dependencies, tuple) or not all(
            isinstance(value, ActionId) for value in self.dependencies
        ):
            raise CompiledGraphError("Action.dependencies must be a tuple of ActionId values")
        object.__setattr__(self, "dependencies", tuple(sorted(set(self.dependencies))))
        if self.query is not None and not isinstance(self.query, str):
            raise CompiledGraphError("Action.query must be a string or None")


@dataclass(frozen=True)
class DependencyGraph:
    actions: tuple[Action, ...]
    external_sources: tuple[ActionId, ...] = field(init=False)
    _nodes: tuple[ActionId, ...] = field(init=False, repr=False)
    _parents: Mapping[ActionId, tuple[ActionId, ...]] = field(init=False, repr=False, compare=False)
    _children: Mapping[ActionId, tuple[ActionId, ...]] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.actions, tuple) or not all(
            isinstance(a, Action) for a in self.actions
        ):
            raise CompiledGraphError("DependencyGraph.actions must be a tuple of Action values")
        actions = tuple(sorted(self.actions, key=lambda action: action.id))
        ids = {action.id for action in actions}
        if len(ids) != len(actions):
            raise CompiledGraphError("DependencyGraph requires unique action ids")
        external = {parent for action in actions for parent in action.dependencies} - ids
        nodes = tuple(sorted(ids | external))
        parents = {node: () for node in nodes}
        children: dict[ActionId, list[ActionId]] = {node: [] for node in nodes}
        for action in actions:
            parents[action.id] = action.dependencies
            for parent in action.dependencies:
                children[parent].append(action.id)
        object.__setattr__(self, "actions", actions)
        object.__setattr__(self, "external_sources", tuple(sorted(external)))
        object.__setattr__(self, "_nodes", nodes)
        object.__setattr__(self, "_parents", MappingProxyType(parents))
        object.__setattr__(
            self,
            "_children",
            MappingProxyType({node: tuple(sorted(values)) for node, values in children.items()}),
        )

    def nodes(self) -> tuple[ActionId, ...]:
        return self._nodes

    def parents(self, id: ActionId) -> tuple[ActionId, ...]:
        try:
            return self._parents[id]
        except KeyError:
            raise UnknownActionError(str(id)) from None

    def children(self, id: ActionId) -> tuple[ActionId, ...]:
        try:
            return self._children[id]
        except KeyError:
            raise UnknownActionError(str(id)) from None

    def descendants(self, id: ActionId) -> tuple[ActionId, ...]:
        pending = deque(self.children(id))
        visited = {id}
        while pending:
            node = pending.popleft()
            if node not in visited:
                visited.add(node)
                pending.extend(self.children(node))
        return tuple(sorted(visited - {id}))

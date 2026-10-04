"""Parse the supported compiled-JSON slice; never invoke Dataform or a provider."""

import json
from collections import Counter
from dataclasses import dataclass

from .types import Action, ActionId, CompiledGraphError, DependencyGraph


@dataclass(frozen=True, order=True)
class CompilationError:
    action: ActionId
    message: str


@dataclass(frozen=True, order=True)
class MissingDependency:
    action: ActionId
    dependency: ActionId
    reason: str


@dataclass(frozen=True)
class GraphReport:
    duplicate_action_ids: tuple[ActionId, ...]
    missing_dependencies: tuple[MissingDependency, ...]
    skipped_action_ids: tuple[ActionId, ...]
    compilation_errors: tuple[CompilationError, ...]
    cycles: tuple[tuple[ActionId, ...], ...]


def _object(value: object, path: str) -> dict:
    if not isinstance(value, dict):
        raise CompiledGraphError(f"{path} must be an object")
    return value


def _array(value: object, path: str) -> list:
    if not isinstance(value, list):
        raise CompiledGraphError(f"{path} must be an array")
    return value


def _target(value: object, path: str) -> ActionId:
    data = _object(value, path)
    try:
        return ActionId(data.get("database"), data.get("schema"), data.get("name"))
    except CompiledGraphError as exc:
        raise CompiledGraphError(f"{path}: {exc}") from exc


def _compilation_errors(data: dict) -> tuple[CompilationError, ...]:
    errors = _object(data.get("graphErrors", {}), "graphErrors")
    if errors.keys() - {"compilationErrors"}:
        raise CompiledGraphError("graphErrors supports only compilationErrors")
    entries = _array(errors.get("compilationErrors", []), "graphErrors.compilationErrors")
    parsed = []
    for index, value in enumerate(entries):
        path = f"graphErrors.compilationErrors[{index}]"
        entry = _object(value, path)
        if "actionTarget" not in entry:
            raise CompiledGraphError(
                f"{path} requires actionTarget; unscoped errors are unsupported"
            )
        target = _target(entry["actionTarget"], f"{path}.actionTarget")
        message = entry.get("message", "")
        if not isinstance(message, str):
            raise CompiledGraphError(f"{path}.message must be a string")
        parsed.append(CompilationError(target, message))
    return tuple(sorted(set(parsed)))


def _cycles(graph: DependencyGraph) -> tuple[tuple[ActionId, ...], ...]:
    """Iterative Kosaraju traversal reports exact strongly connected components."""
    visited: set[ActionId] = set()
    finished: list[ActionId] = []
    for root in graph.nodes():
        if root in visited:
            continue
        visited.add(root)
        stack = [(root, iter(graph.children(root)))]
        while stack:
            node, children = stack[-1]
            child = next(children, None)
            if child is None:
                finished.append(node)
                stack.pop()
            elif child not in visited:
                visited.add(child)
                stack.append((child, iter(graph.children(child))))

    assigned: set[ActionId] = set()
    cycles = []
    for root in reversed(finished):
        if root in assigned:
            continue
        component: set[ActionId] = set()
        pending = [root]
        assigned.add(root)
        while pending:
            node = pending.pop()
            component.add(node)
            for parent in graph.parents(node):
                if parent not in assigned:
                    assigned.add(parent)
                    pending.append(parent)
        if len(component) > 1 or root in graph.parents(root):
            cycles.append(tuple(sorted(component)))
    return tuple(sorted(cycles))


def load_compiled_graph(text_or_dict: str | dict) -> tuple[DependencyGraph, GraphReport]:
    """Load enabled table/view/incremental actions and report excluded definitions."""
    if isinstance(text_or_dict, str):
        try:
            data = json.loads(text_or_dict.removeprefix("\ufeff"))
        except (ValueError, RecursionError) as exc:
            raise CompiledGraphError(f"Invalid compiled graph JSON: {exc}") from exc
    else:
        data = text_or_dict
    data = _object(data, "compiled graph")
    if "tables" not in data:
        raise CompiledGraphError("compiled graph requires tables")
    tables = _array(data["tables"], "tables")
    for section in (
        "operations",
        "assertions",
        "declarations",
        "tests",
        "notebooks",
        "dataPreparations",
        "propertyGraphs",
    ):
        if section in data and _array(data[section], section):
            raise CompiledGraphError(
                f"Nonempty {section} is unsupported; only tables are supported"
            )

    entries = []
    for index, value in enumerate(tables):
        path = f"tables[{index}]"
        entry = _object(value, path)
        entries.append((_target(entry.get("target"), f"{path}.target"), entry, path))

    counts = Counter(target for target, _, _ in entries)
    duplicates = {target for target, count in counts.items() if count > 1}
    errors = _compilation_errors(data)
    error_targets = {error.action for error in errors}
    skipped = error_targets & counts.keys()
    actions = []
    for target, entry, path in entries:
        if target in duplicates or target in skipped:
            continue
        if type(entry.get("disabled", False)) is not bool or entry.get("disabled", False):
            raise CompiledGraphError(f"{path}.disabled must be false or absent")
        dependency_values = _array(entry.get("dependencyTargets", []), f"{path}.dependencyTargets")
        dependencies = tuple(
            _target(value, f"{path}.dependencyTargets[{index}]")
            for index, value in enumerate(dependency_values)
        )
        try:
            actions.append(Action(target, entry.get("type"), dependencies, entry.get("query")))
        except CompiledGraphError as exc:
            raise CompiledGraphError(f"{path}: {exc}") from exc

    graph = DependencyGraph(tuple(actions))
    missing = []
    external_sources = set(graph.external_sources)
    for action in graph.actions:
        for dependency in action.dependencies:
            if dependency in external_sources:
                reason = (
                    "graph_error"
                    if dependency in error_targets
                    else "duplicate"
                    if dependency in duplicates
                    else "absent"
                )
                missing.append(MissingDependency(action.id, dependency, reason))
    report = GraphReport(
        duplicate_action_ids=tuple(sorted(duplicates)),
        missing_dependencies=tuple(sorted(missing)),
        skipped_action_ids=tuple(sorted(skipped)),
        compilation_errors=errors,
        cycles=_cycles(graph),
    )
    return graph, report

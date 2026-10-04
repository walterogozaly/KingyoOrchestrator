# Offline compiled Dataform graph ingestion

`load_compiled_graph(text_or_dict)` in `kingyo_orchestrator.graph` accepts a JSON
string (including a leading UTF-8 BOM) or a dictionary. It returns
`(DependencyGraph, GraphReport)`. It never runs Dataform, reads files, accesses
credentials, imports a provider SDK, or evaluates SQL. The caller supplies the
input; loading a fixture from disk is separate from parsing it.

## Supported compiled shape

This slice supports a top-level `tables` array. Each enabled action has an explicit
`target` with `database`, `schema`, and `name`; a `type` of `table`, `view`,
or `incremental`; optional `dependencyTargets` (an array of explicit targets,
defaulting to empty); and optional `query` (a string or null).
Identifiers follow `[A-Za-z_][A-Za-z0-9_]*`, with hyphens additionally allowed
after the first character in the database component. There are no default
database/schema values, aliases, or SQLX parsing.

`graphErrors` is absent, empty, or an object whose only supported key is
`compilationErrors` (an array). Each error must have a usable `actionTarget`;
its `message` is an optional string, defaulting to empty. The shape follows
Dataform's [compiled graph and compilation-error definitions](https://github.com/dataform-co/dataform/blob/main/protos/core.proto).
Error targets are parsed before action bodies, so an action with a compilation
error can be skipped even if its partially compiled type/query is unusable.
Deprecated `actionName` or file-only errors cannot identify an action reliably
and are rejected rather than guessed. Unknown graph-error categories are rejected.

Nonempty `operations`, `assertions`, `declarations`, `tests`, `notebooks`,
`dataPreparations`, or `propertyGraphs` collections are unsupported and rejected;
empty arrays are accepted. `disabled` must be false or absent on retained actions.
Enum-only action types, missing targets, invalid dependencies, and unsupported
payload shapes raise `CompiledGraphError` naming the field or array index.
Unconsumed descriptive metadata (for example, tags or project configuration)
is ignored. This is deliberately a narrow compiled-table interface, not a
complete parser for every Dataform version or output format.

## Immutable graph and diagnostics

`ActionId(database, schema, name)` is frozen, ordered, and hashable.
`str(id)` yields `database.schema.name`. `Action` is frozen and contains
`id`, `type`, a sorted unique tuple of dependency IDs, and optional query text.
Query text is opaque and is never executed or analyzed.

`DependencyGraph` copies actions into a sorted tuple and builds read-only
adjacency indexes. Its `nodes()` includes retained actions and explicit external
source nodes. `external_sources` contains referenced IDs with no retained action.
`parents(id)` returns immediate upstream IDs; `children(id)` returns immediate
downstream IDs. `descendants(id)` traverses downstream iteratively, de-duplicates
nodes, excludes the requested node itself, and returns IDs in lexical order
(not build order). Unknown IDs raise `UnknownActionError`. Even a cyclic graph
can be inspected without an infinite traversal.

`GraphReport` is frozen and contains deterministic tuples:

| Field | Meaning |
| --- | --- |
| `duplicate_action_ids` | IDs with multiple definitions; all their definitions are excluded, so input order never selects a winner. |
| `missing_dependencies` | `MissingDependency(action, dependency, reason)` for each retained action's unresolved edge; reason is `absent`, `duplicate`, or `graph_error`. |
| `skipped_action_ids` | IDs actually present in tables and excluded due to compilation errors. |
| `compilation_errors` | Unique `CompilationError(action, message)` records, including targets that have no table entry. |
| `cycles` | Sorted groups of exact cycle members (strongly connected components), including self-loops; downstream nonmembers are excluded. |

References to skipped or duplicate definitions remain explicit external nodes,
with their reason recorded. Such nodes are unresolved placeholders, not proof of
valid upstream tables. Inspect the report before using this graph for planning;
cycle and compilation-error reports do not produce a valid execution plan.
The loader retains cycles for diagnostics, and its iterative cycle detection
works on graphs deeper than Python's recursion limit.

## Try with synthetic data

```python
from kingyo_orchestrator.graph import ActionId, load_compiled_graph

source = {"database": "project_x", "schema": "dataset_a", "name": "table_orders"}
output = {"database": "project_x", "schema": "dataset_a", "name": "table_clean"}
graph, report = load_compiled_graph({
    "tables": [{
        "target": output,
        "type": "table",
        "dependencyTargets": [source],
        "query": "SELECT 1 AS order_id",
    }],
    "graphErrors": {"compilationErrors": []},
})
source_id = ActionId(**source)
output_id = ActionId(**output)
assert graph.parents(output_id) == (source_id,)
assert graph.descendants(source_id) == (output_id,)
assert graph.external_sources == (source_id,)
assert report.missing_dependencies[0].reason == "absent"
assert report.cycles == ()
```

The hand-written `tests/fixtures/compiled_graph.json` demonstrates a diamond and
an external source using only synthetic identifiers. Tests also cover duplicates,
error skips, BOM input, malformed shapes, input mutation, and deep cycles/chains.

A string-ID planner can adapt this graph with a local map
`ids = {str(id): id for id in graph.nodes()}`: expose `tuple(ids)` as its nodes,
map a requested string through `ids`, call `parents` or `children`, then
convert returned IDs with `str`. The graph includes external nodes so sources
remain traversable. This slice does not wire that adapter into a planner,
decide which actions to rebuild, supply build order, persist state, or add CLI
commands.

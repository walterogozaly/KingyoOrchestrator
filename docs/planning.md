# Offline impact planning

`planning.plan(graph, changed_sources)` proposes every proper descendant of each
known changed source. It returns data only: there is no metadata polling, SQL
parsing, provider SDK, filesystem access, or execution. The CLI is not wired to it.

## Input contract

`Graph` is a minimal protocol with `nodes()`, `parents(node)`, and `children(node)`;
each method returns an iterable of nonempty string ids. `nodes()` must include all
nodes, including external sources, exactly once. Parent and child views must agree,
and edges must not refer to absent nodes. Duplicate adjacency entries are harmless.
The planner copies adjacency once; the caller must supply a stable graph for that call.
Invalid ids, dangling adjacency, and inconsistent views raise `GraphValidationError`.

`changed_sources` is an iterable of ids, not a single string. Repeated ids collapse
to one source. Unknown changed ids are reported without preventing known impacts.
The entire graph must be acyclic, including untouched branches and empty change
sets. `GraphCycleError` reports sorted `unresolved_nodes`: these include cycle nodes
and any descendants blocked by the cycle, rather than claiming to identify only
cycle members. Traversal and ordering use iterative algorithms.

## Output and supported policy

`Plan` and `PlanStep` are frozen dataclasses with tuple collections:

- `Plan.steps`: affected actions, each appearing once, in topological order.
  Among currently ready affected actions, the alphabetically first id is selected.
  Unaffected prerequisites remain outside the plan; their existing results are assumed usable.
- `Plan.unknown_sources`: sorted, deduplicated changed ids absent from the graph.
- `PlanStep.action`: the affected node's string id.
- `PlanStep.triggered_by`: all known changed sources that reach this action through
  at least one edge, sorted alphabetically.
- `PlanStep.reason`: a human-readable changed source and one shortest dependency
  path. Shortest means fewest edges across all triggering sources; equal-length
  paths are tied by alphabetical order of the complete id sequence.

A changed source itself is not a step just because it changed. It is included if
another changed source reaches it. A changed node with no descendants gives no
steps. Empty changes give an empty plan. Unchanged branches never appear.

## Runnable synthetic example

```python
from kingyo_orchestrator.planning import plan

class MemoryGraph:
    dependencies = {
        "source_orders": (),
        "view_orders": ("source_orders",),
        "table_daily": ("view_orders",),
        "source_customers": (),
    }

    def nodes(self):
        return tuple(self.dependencies)

    def parents(self, node):
        return self.dependencies[node]

    def children(self, node):
        return tuple(child for child, parents in self.dependencies.items() if node in parents)

result = plan(MemoryGraph(), {"source_orders", "source_unknown"})
print(result.unknown_sources)  # ("source_unknown",)
for step in result.steps:
    print(step.action, step.triggered_by, step.reason)
# view_orders ('source_orders',) Changed source source_orders via source_orders -> view_orders
# table_daily ('source_orders',) Changed source source_orders via source_orders -> view_orders -> table_daily
```

## Graph ingestion adaptation and limits

The planning protocol does not depend on `graph/` or Dataform's compiled JSON.
To adapt the ingestion graph, map every action and external source to a unique,
stable string, translate its parent/child lookups through that map, and expose all
ids through `nodes()`. Supply changed sources in that same id scheme. Cycles or
incomplete ingestion reports should be resolved before trusting a plan; the
planner validates the graph it is given, not whether compilation omitted actions.

The current policy deliberately rebuilds all downstream nodes. It does not estimate
cost, refine partitions or changed columns, evaluate SQL, persist results, or decide
whether to invoke a workflow. Integration with the metadata, state, and compiled
graph packages, richer readiness policies, and CLI wiring are future work.

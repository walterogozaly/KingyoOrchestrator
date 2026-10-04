# Per-consumer column relevance

The question this module answers is not "is this an audit column?" but **"is any
column that changed actually used downstream?"** For each consumer of a changed
table, `relevance()` answers with `relevant` or `no_op`, keyed by consumer. One
change can be relevant to one child and a no-op for the next, so there is no
single global verdict and never one: a column that nobody reads in an audit
mart may be the join key of a daily mart.

The order of failure matters. A wrong `relevant` costs one rebuild. A wrong
`no_op` silently skips work that was needed, which is the worst thing an
orchestrator can do. Nothing here guesses toward `no_op`.

## The pieces

| Piece | Module | Depends on sqlglot |
| --- | --- | --- |
| `ColumnUsage`, `ConsumerRelevance`, `relevance` | `core/column_usage.py` | no |
| `SqlglotColumnUsageSource`, `StaticColumnCatalog` | `lineage/sqlglot_source.py` | yes |

```python
from kingyo_orchestrator.core.column_usage import ConsumerAction, relevance
from kingyo_orchestrator.lineage import SqlglotColumnUsageSource

orders = "project_x.dataset_a.table_orders"
graph = {
    orders: ConsumerAction(id=orders, query="SELECT 1"),
    "dataset_a.table_daily": ConsumerAction(
        id="dataset_a.table_daily",
        query="SELECT order_id, amount FROM table_orders",
        depends_on=(orders,),
    ),
    "dataset_a.table_counts": ConsumerAction(
        id="dataset_a.table_counts",
        query="SELECT count(*) AS n FROM table_orders",
        depends_on=(orders,),
    ),
}
results = relevance(graph, orders, ("_fingerprint",), source=SqlglotColumnUsageSource())
assert results["dataset_a.table_daily"].verdict == "no_op"
assert results["dataset_a.table_counts"].verdict == "no_op"
```

`changed_keys`, when supplied, are changed key values from the fingerprint work: a
consumer that reads a changed key is `relevant` even if it reads none of the
changed columns, because its rows can move.

## What counts as used

A column is used by a consumer when it appears anywhere in that consumer's query
as a source reference: selected, filtered (`WHERE`, `QUALIFY`, `HAVING`), joined
(`ON`, `USING`), grouped, ordered, or used inside a window clause or a function
argument. References are followed through CTEs, subqueries, and table aliases to
the underlying table, so a column consumed only inside a CTE still counts.

Names compare case-insensitively, as BigQuery identifiers do. `count(*)` reads
rows rather than a column, so it does not mark anything used.

## The conservative fallback

A column's usage is only reported when it can be attributed to exactly one source
table. Everything else becomes `ColumnUsage.all_used(reason)`: every column counts,
the consumer is `relevant`, and the reason string says what happened.

| Case | Why it cannot be attributed |
| --- | --- |
| `SELECT *` or `SELECT t.*` | the projected columns are not written in the query |
| SQL that does not parse | nothing can be read from it |
| no query text on the action | there is nothing to analyze |
| an unqualified column with several sources in scope and no catalog | the source is undecidable |
| an unqualified column that two sources declare | ambiguous by construction |
| an alias that matches no source in scope | the reference cannot be resolved |
| `UNNEST`, `PIVOT`, `UNPIVOT`, `LATERAL` in `FROM` | the row shape is not modeled |
| a derived source that does not expose the referenced name | no lineage to follow |

Two rules protect against silent wrong answers that are not about parsing:

- **The changed table must be visible in the analysis.** If a consumer declares a
  dependency on a table that its own analysis never mentions, the verdict is
  `relevant`, not `no_op`: the consumer's SQL may simply name that table
  differently from the graph, and a naming mismatch must not read as "unused".
- **A table that is read but whose columns are never named still counts as seen.**
  `SELECT count(*) FROM table_orders` reads no column and skips nothing, which is a
  real `no_op`, not an analysis failure.

An optional `ColumnCatalog` makes unqualified columns attributable in joins:
`SqlglotColumnUsageSource(catalog)` attributes a column declared by exactly one
source and reports one declared by two as ambiguous. Without a catalog, a join
whose key is unqualified stays unresolved, which is the safe direction.

## Optional dependency

sqlglot is imported lazily, inside the calls that need it:

```console
pip install 'kingyo-orchestrator[lineage]'
```

Importing `kingyo_orchestrator.lineage` or `kingyo_orchestrator.core.column_usage`
without the extra works; only constructing the source and calling it needs
sqlglot, and a missing dependency raises `LineageDependencyError` with the install
command rather than an `ImportError` from inside a walk. Tests skip when it is
absent. KumoSQL has its own lineage and may replace this later, which is why
everything above talks to the `ColumnUsageSource` protocol and never to sqlglot.

## Adapting a compiled graph

`relevance()` takes a mapping of action id to action, where an action exposes `id`,
`query`, and `depends_on`. `kingyo_orchestrator.graph.Action` satisfies this through
a thin adapter, since its ids are `ActionId` values:

```python
graph, report = load_compiled_graph(payload)
actions = {
    str(action.id): ConsumerAction(
        id=str(action.id),
        query=action.query,
        depends_on=tuple(str(dependency) for dependency in action.dependencies),
    )
    for action in graph.actions
}
```

`table` is matched to a SQL table reference by table name, not by project and
dataset, so `dataset_a.table_orders` and `table_orders` are the same table here. A
query reading `other_project.dataset_a.table_orders` therefore counts as reading
the changed table: that direction can only add a rebuild.

## Interaction with the fingerprint work

The fingerprint module detects a content change in a small dimension table and
reports which columns it hashed. Two points meet here, and neither changes the
other module:

1. **Configured and detected audit columns feed in as changed columns that nobody
   reads.** Pass them as `changed_columns`; consumers that do not reference them
   come back `no_op`, which is the generalization of the configured audit-column
   rule.
2. **The fingerprint should hash only columns at least one consumer reads.** This
   is the direction that saves work and belongs to the fingerprint module: a
   column nobody reads cannot change any consumer's output, so hashing it wastes
   bytes. `relevance()` reports the unread columns for that purpose; nothing in
   `fingerprints/` reads it yet, and no code there was changed by this work.

## Limits

- **One hop.** Only direct consumers of the changed table are reported. A change
  that reaches a grandchild only through an intermediate action is a documented
  follow-up: it needs the intermediate action's own usage to decide whether the
  change survives one more hop, and an intermediate that reads nothing can stop
  the walk. Multi-hop is not implemented and must not be approximated by walking
  descendants and reporting their relevance.
- **Plain SQL after compilation.** Templated or JS-generated SQL that is not plain
  SQL when it reaches this module is unresolved, which is safe.
- **Read, not needed.** A column read inside a CTE that no outer query consumes is
  still counted as read. That matches what the warehouse has to read, and errs
  toward `relevant`.
- **No schemas, no samples, no queries.** Nothing here executes SQL or talks to a
  warehouse, and no column is inferred from data.
# Per-consumer column relevance

The question Kingyo needs answered for every change is not "is this the audit column?" but:

> **Is any column that changed actually used downstream?**

The answer is per consumer. One change can matter to one child and be a no-op for another, so the
result is keyed by child and is never a single global verdict.

## Interface

```python
from kingyo_orchestrator.lineage import relevance

answers = relevance(graph, changed_table, ["ingested_at"], changed_keys=None)
for child, answer in answers.items():
    answer.relevant      # True -> the child must run
    answer.no_op         # True -> the child reads none of the change and can be skipped
    answer.reason        # always populated; every no_op is explainable
    answer.columns       # changed columns this child reads
    answer.keys          # changed keys this child reads, when keys were supplied
    answer.unresolved    # True -> usage could not be resolved, so relevant by default
```

`changed_table` and the keys of the result are `graph.ActionId` values from the compiled-graph loader,
so a `str(ActionId)` such as `example-project.analytics.orders` matches a query that says
`orders`, `analytics.orders` or `` `example-project.analytics.orders` ``.

## What counts as "used"

A column counts as used by a consumer when it appears anywhere in that consumer's query as a source
reference:

| Where | Example |
| --- | --- |
| selected | `SELECT o.amount` |
| filtered | `WHERE o.loaded_at > x` |
| joined | `JOIN c ON c.orders_id = o.id`, `JOIN c USING (custkey)` |
| grouped or aggregated | `GROUP BY o.custkey`, `sum(o.amount)` |
| ordered | `ORDER BY o.ts` |
| window or `QUALIFY` | `PARTITION BY o.custkey ORDER BY o.ts`, `QUALIFY rn = 1` |
| through a CTE, subquery or alias | `WITH s AS (SELECT amount FROM orders) SELECT amount FROM s` |

References are resolved per scope, so a column used only inside a CTE or a correlated subquery still
counts, and a correlated reference to an enclosing scope's table is credited to that table.

## The conservative fallback

When a consumer's column usage cannot be resolved, **every column is treated as used** and
`answer.reason` says why. These cases return `unresolved`:

- `SELECT *` or `SELECT t.*` on a physical table (the columns cannot be enumerated)
- an unqualified column with two or more joined sources (no schema is available to disambiguate)
- SQL that does not parse as BigQuery
- text that is not plain SQL after compile (`${...}`, `self()`, templated or generated SQL)
- an unresolvable table alias, a column reference with no resolvable name, or a query that is missing

A `SELECT *` over a CTE is *not* unresolved when the CTE body is plain SQL, because that body is
traversed in its own right.

The direction is deliberate. A false "relevant" costs a rebuild. A false "no_op" silently skips work
that was needed, which is the worst failure an orchestrator can have. The checker therefore never
infers `no_op` when it is unsure, and it errs towards over-attribution (crediting a column to a table
it does not come from) rather than under-attribution.

## Optional dependency

The sqlglot-backed source is imported lazily and lives behind an extra:

```shell
pip install -e ".[lineage]"
```

Without it, `SqlglotColumnUsage` raises `LineageDependencyError` with the install hint rather than a bare
`ImportError`. Nothing else in Kingyo needs the parser.

## Swapping the lineage source

Only `ColumnUsage` crosses the boundary:

```python
class ColumnUsageSource(Protocol):
    def column_usage(self, action) -> ColumnUsage: ...
```

`relevance(..., source=...)` takes any implementation, so KumoSQL's own lineage can replace the sqlglot
one later without touching the planner. A source that raises is treated as unresolved, again
conservatively.

## Interaction with dimension fingerprints

[#12](dimension-fingerprints.md) detects content changes in small dimension tables using a content
fingerprint over a configured column list, with detected audit columns excluded. This issue does not
change that code; it generalises the same idea downstream:

- #12's configured and detected audit columns are *inputs* here: pass them as `changed_columns` and
  every child that reads none of them comes back `no_op`, because nobody reads an audit column. The
  special case falls out of the general rule instead of being configured.
- `changed_keys` carries the key columns from #12, so a consumer keyed on them is marked relevant for
  a row that moved between keys.
- The fingerprint should hash only columns that at least one child reads. Hashing an audit column makes
  the fingerprint change whenever the audit column changes, which then looks like a real content change
  downstream. That is the projection rule from Walter's cost rules applied to hashing.

## Limits

- **One hop.** A column used transitively through an intermediate table is attributed to the immediate
  parent only. Deciding relevance for grandchildren needs a second pass with the intermediate table's own
  column set. Multi-hop column lineage is a documented follow-up.
- **Table identity is by name.** `orders` in two different datasets of the same project are treated as
  one table. The compiled graph carries the full `database.schema.name`, and this is the same
  string-identity caveat the impact planner has.
- **`count(*)` reads no column.** A change that only affects row counts is not a column change and is
  out of scope here; row-level changes are handled by partition selection.
- Output aliases are only recognised in `ORDER BY` and `QUALIFY`. A `GROUP BY` naming an output alias
  is credited to the source table as well, which over-attributes rather than under-attributes.
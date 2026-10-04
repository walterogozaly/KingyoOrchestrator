# Offline metadata interface

The `kingyo_orchestrator.metadata` package defines immutable observations and a
read-only `MetadataReader` protocol. It contains no provider SDK, network calls,
credential loading, or project discovery. The CLI still validates local settings;
this interface is not wired into polling or persistence.

`TableRef(project, dataset, table)` requires three explicit, nonempty strings.
It is frozen and hashable, so it can key the fake reader's seed dictionary.
`TableSnapshot(ref, modified_time, num_rows, partitioning_column=None)` is frozen.
It requires a timezone-aware Python datetime and a nonnegative integer row count,
and normalizes timestamps to UTC. A partitioning column is a nonempty string or
`None` when no column is reported (for example, an unpartitioned or ingestion-time
partitioned table). This slice does not represent unknown timestamps or row counts,
partition granularity, schema changes, or change classification.

Both types provide `to_dict()` and `from_dict()`. A snapshot's JSON-safe dictionary
has exactly `ref` (a dictionary with `project`, `dataset`, `table`),
`modified_time` (an ISO 8601 string with UTC offset), `num_rows` (integer), and
`partitioning_column` (string or null). Decoding rejects missing/extra fields and
invalid values with `ValueError`.

## Try it offline

```python
import json
from datetime import UTC, datetime

from kingyo_orchestrator.metadata import (
    FakeMetadataReader,
    TableRef,
    TableSnapshot,
)

ref = TableRef("project_x", "dataset_a", "table_orders")
initial = TableSnapshot(ref, datetime(2026, 1, 1, tzinfo=UTC), 42, "order_sold_ts")
reader = FakeMetadataReader({ref: initial})
assert reader.get_snapshot(ref) == initial
assert TableSnapshot.from_dict(json.loads(json.dumps(initial.to_dict()))) == initial

updated = TableSnapshot(ref, datetime(2026, 1, 2, tzinfo=UTC), 43, "order_sold_ts")
reader.set_snapshot(updated)
assert reader.get_snapshot(ref) == updated
assert initial.num_rows == 42
```

`FakeMetadataReader` copies the seed mapping and requires each key to match its
snapshot's reference. Repeated reads are stable until `set_snapshot(snapshot)`
seeds or replaces a table in memory. Missing tables raise `TableNotFoundError`
(a `LookupError`) with the requested `ref` attribute. The fake has no disk state.

A future provider adapter can satisfy `MetadataReader.get_snapshot(ref)` using
these values while keeping provider objects outside orchestration decisions.
Metadata changes are signals, not proof of newly arrived rows. Real metadata reads,
cloud authorization, persistence, comparison, and CLI polling are separate work.

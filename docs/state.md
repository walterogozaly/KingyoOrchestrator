# Local snapshot checkpoints

`SnapshotStore` persists observations in a caller-selected JSON file. `compare`
is a pure decision function in `core/`; file access stays in `state/`. Neither
component reads metadata, loads credentials, executes SQL, or contacts a provider.
The CLI does not use the store yet.

## Supported snapshot format

`Snapshot` is a minimal typed dictionary with `modified_time` (a timezone-aware
ISO 8601 string) and `num_rows` (a nonnegative integer, excluding booleans).
`SnapshotState` maps nonempty string table ids to these snapshots. Table ids are
opaque keys supplied explicitly by the caller; use a consistent spelling on every
run. Timestamps normalize to UTC, so equivalent offsets do not signal changes.
Additional observation fields are ignored and are not persisted.

The UTF-8 state file has exactly two top-level fields:

```json
{
  "version": 1,
  "snapshots": {
    "project_x.dataset_a.table_orders": {
      "modified_time": "2026-01-01T12:00:00+00:00",
      "num_rows": 42
    }
  }
}
```

A missing file returns `{}` without creating a file or directory. Invalid JSON,
invalid encoding, duplicate keys, unsupported versions, and invalid snapshot fields
raise `StateFormatError`; loading never silently resets corrupt state. Other file
errors propagate. The caller must resolve corruption explicitly before continuing.

## Offline use

```python
from pathlib import Path

from kingyo_orchestrator.core.changes import compare
from kingyo_orchestrator.state import SnapshotStore

store = SnapshotStore(Path("var/snapshots.json"))  # var/ is ignored by Git
previous = store.load()  # reread disk on each scheduled run
table_id = "project_x.dataset_a.table_orders"
current = {"modified_time": "2026-01-01T12:00:00+00:00", "num_rows": 42}
status = compare(previous.get(table_id), current)
previous[table_id] = current
store.save(previous)
print(status)  # first_seen initially, unchanged on the next run
```

`compare(None, current)` returns `first_seen`. For a known table, only a difference
in modified time or row count returns `changed`; otherwise it returns `unchanged`.
Both increases and decreases count. Metadata differences do not prove new rows
arrived and do not schedule execution.

The minimal state types do not depend on the metadata package. To adapt a
`TableSnapshot`, build a stable key from its explicit `TableRef` fields, such as
`f"{ref.project}.{ref.dataset}.{ref.table}"`, and pass `snapshot.to_dict()` to
`compare` and `save`. The two change fields are projected automatically; `ref`
and `partitioning_column` are ignored. Offline tests exercise this with the fake reader.

## Write and restart limits

`save` validates all input, creates missing parent directories, writes and flushes
a temporary file beside the destination, then uses `os.replace` to atomically
replace the complete checkpoint. A failed write leaves the old checkpoint intact;
normal exceptions clean up the temporary file. A process killed before replacement
may leave a temporary file, which `load` ignores. Atomicity assumes a filesystem
supporting atomic replacement; this is not a guarantee against power loss.

`save` replaces all state; retain prior entries when saving a subset of observations.
After a successful save, a fresh store reports the same observation as `unchanged`.
If the process stops before saving, that observation can be reported again. There is
no exactly-once guarantee for downstream side effects and no multi-writer locking;
use one writer per file. Scheduling, pruning, and downstream execution are outside
this API.

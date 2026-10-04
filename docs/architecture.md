# Initial architecture

Kingyo is intended to observe source changes, decide which downstream work may
be affected, and eventually coordinate approved work. The current implementation
validates local settings and offers an offline metadata contract with a fake
reader. The remaining architecture below is a proposed direction.

## Boundaries

| Area | Responsibility | First likely implementation |
| --- | --- | --- |
| CLI/configuration | Explicit settings and local entry points | Existing `cli.py` and `config.py` |
| Metadata contract | Immutable observations and read-only reader protocol | Existing `metadata/`; see [interface](metadata.md) |
| Core | Change comparison, dependency traversal, proposed work | Pure functions under `core/` |
| Adapters | BigQuery metadata, Dataform compiled graphs, KumoSQL integration | Provider-specific modules under `adapters/` |
| State | Previous observations, checkpoints, run outcomes | A local store under `state/`, data in ignored `var/` |

Core decisions should consume ordinary data structures rather than cloud SDK
objects. Keep network calls at adapter boundaries so tests can use synthetic data.
Choose dependencies when implementing an adapter; the bootstrap has none.

## Proposed observation flow

1. Load settings with an explicit project and location.
2. Read source metadata through an adapter.
3. Compare it with a stored snapshot.
4. Traverse a dependency graph to propose affected downstream actions.
5. Save the observation and report the proposal.

Metadata changes are signals, not proof that new rows arrived. Distinguish data,
schema, and uncertain changes where provider metadata permits. Treat invalid or
incomplete compiled graphs as uncertain input and surface that uncertainty.

## Execution boundary

Observation and execution should have separate entry points. Before adding
execution, define explicit approval, idempotency, retry behavior, concurrency,
cost limits, and audit records. Do not turn observation into automatic execution
by changing a default setting.

Store credentials through provider-supported mechanisms outside the repository.
Public examples use placeholders, and local environment notes stay ignored.

# Initial work sequence

The repository setup is complete; these are suggested next implementation steps.

1. **Read-only metadata adapter.** Accept explicit source identifiers and return
   stable snapshots. Test using fake responses before checking against a local
   environment. Keep provider SDKs optional until needed.
2. **Snapshot persistence and comparison.** Store observations locally, distinguish
   first observations from changes, and handle restarts without duplicate signals.
3. **Dependency ingestion.** Accept a compiled Dataform graph or an agreed KumoSQL
   interface. Report invalid actions and missing dependencies explicitly.
4. **Offline planning.** Given changes and a graph, report affected actions with
   reasons. Cover cycles, missing nodes, and unchanged sources using synthetic fixtures.
5. **Observation loop.** Add one-shot and polling commands with clear shutdown,
   checkpointing, and error handling.
6. **Execution design.** Agree on approval, costs, concurrency, idempotency, and
   execution identity before implementing any writes or workflow invocations.

The precise orchestration policy and deployment target are still open decisions.

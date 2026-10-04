# State signal audit

`test_state_signals.py` records the offline audit from issue #26. It changes no
production behavior. All references and observations are synthetic.

`test_column_usage_fallback.py` records the offline audit from issue #47, which
attacks the column-relevance analyzer from #13 for the soundness direction named
there: a `no_op` verdict for a child that actually reads a changed column. It
also changes no production behavior.

Run the checks normally:

```console
python -m pytest tests/audit/test_state_signals.py -q
python -m pytest tests/audit/test_column_usage_fallback.py -q
```

To replay the reported defects as ordinary failures:

```console
python -m pytest tests/audit/test_state_signals.py --runxfail -q
python -m pytest tests/audit/test_column_usage_fallback.py --runxfail -q
```

The initial audit produced 20 passing checks and nine expected failures over
three root causes. On non-Windows systems, the native sharing-mode check skips.
The expected failures are strict: a fix causes an unexpected pass until its
marker is removed, rather than silently leaving a resolved witness marked broken.

| Finding | Witness coverage |
| --- | --- |
| [#32](https://github.com/walterogozaly/KingyoOrchestrator/issues/32) | Distinct fractional instants collapse during pure comparison and save/load. Explicit rejection of unsupported precision is also acceptable. |
| [#33](https://github.com/walterogozaly/KingyoOrchestrator/issues/33) | Distinct accepted references collide through the documented dot-joined key recipe. These components are accepted by this API; provider validity is not claimed. |
| [#34](https://github.com/walterogozaly/KingyoOrchestrator/issues/34) | Lower/upper UTC range overflow escapes state-load, comparison, and metadata validation error types. |

The passing probes cover microsecond changes, backwards timestamps, equivalent
offsets, naive/leap-second rejection, first observation after deletion/rename,
opaque case/whitespace keys, BOM/truncation/duplicate/version rejection, a bounded
20,000-table round trip, write denial, an actual hard process exit before replace,
and a native Windows open handle without delete sharing. Permission denial is
injected at temporary-file creation; it does not claim to test OS directory ACLs.

Other passing probes demonstrate documented limitations: equal metadata cannot
detect changed content, a crash before saving repeats a signal, a save before a
downstream action does not record pending work, and interleaved writers can
overwrite each other's checkpoints. These are observations of the existing
single-writer observation API, not an execution or acknowledgement protocol.

This audit does not test power loss, every filesystem, unbounded resource
exhaustion, cloud metadata precision, or provider identifier normalization.
CI exercises Python 3.11, 3.12, and 3.13; the Windows-only check needs Windows.

## Column relevance audit

`test_column_usage_fallback.py` covers [#47](https://github.com/walterogozaly/KingyoOrchestrator/issues/47).
It holds 18 parametrized no-op-risk cases that each name a column the consumer really reads, plus the
derivations, joins, aggregates, window clauses, struct access, `UNION ALL`, CTEs and subqueries they go
through. Every one of those currently resolves, so a false `no_op` was not found: that is the direction
#13 called the worst failure, and the clean checks are now locked in as regression tests.

The expected failures are the finding: a derived source that cannot be attributed raises
`AttributeError` instead of returning the conservative `all_used` fallback, and `relevance` has no guard,
so one such child aborts the planning call. Six query shapes reach that one root cause.

Three conservative gaps are asserted to stay conservative, so a change that quietly resolved them
without a decision gets noticed: a correlated reference to an enclosing scope's table, a correlated
scalar subquery, and a CTE joined to itself. All three return `unresolved` today, which costs a rebuild
rather than skipping work.

It does not test multi-hop lineage, sqlglot version drift beyond the pinned one, or the BigQuery
behaviours a real `SELECT * EXCEPT` or `PIVOT` would take.

# State signal audit

`test_state_signals.py` records the offline audit from issue #26. It changes no
production behavior. All references and observations are synthetic.

Run the checks normally:

```console
python -m pytest tests/audit/test_state_signals.py -q
```

To replay the reported defects as ordinary failures:

```console
python -m pytest tests/audit/test_state_signals.py --runxfail -q
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

# Benchmark report: 6 trials across 4 scenarios

Totals: faster=1, same=1, slower=1, incorrect=1, failed=1, unsupported=1.

Ratios are candidate / baseline (lower is better); speedup is baseline / candidate.
Metric cells show geomean ratio [min, max]; geomean speedup; contributing trial count.
Correct trials only; faster < 0.95, slower > 1.05.

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | rows_scanned | seconds |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| case_a | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 0.707 [0.500, 1.000]; 1.414x; n=2 | 0.707 [0.500, 1.000]; 1.414x; n=2 |
| case_b | 2 | 0 | 0 | 1 | 1 | 0 | 0 | 2.000 [2.000, 2.000]; 0.500x; n=1 | 2.000 [2.000, 2.000]; 0.500x; n=1 |
| case_c | 1 | 0 | 0 | 0 | 0 | 1 | 0 | - | - |
| case_d | 1 | 0 | 0 | 0 | 0 | 0 | 1 | - | - |

## Non-correct trials

| scenario | seed | status | detail |
|---|---|---|---|
| case_b | 1 | incorrect | duplicate row |
| case_c | 0 | failed | candidate raised ValueError |
| case_d | 0 | unsupported | missing capability |

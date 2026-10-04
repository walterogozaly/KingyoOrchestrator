# Prototype vs full rebuild, SF 0.05, 3 seeds, 1 timing repeat (DuckDB zone-map proxy, not BigQuery)

## kingyo-prototype

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| unchanged_no_signal | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.000 | 0.01 | 3,222,061 | 0 |
| unchanged_resignal | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.197 | 0.58 | 3,222,061 | 635,530 |
| new_day | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.204 | 0.63 | 3,227,461 | 659,441 |
| backfill_7_days | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.204 | 0.43 | 3,227,416 | 659,305 |
| late_updates_recent | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.237 | 0.60 | 3,222,061 | 763,913 |
| late_updates_scattered | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 2.364 | 2.42 | 3,222,061 | 7,617,010 |
| dimension_change | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 1.814 | 1.57 | 3,222,061 | 5,845,818 |
| source_column_added | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 2.001 | 1.24 | 3,222,061 | 6,445,826 |
| edge_mix | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 3.994 | 1.23 | 3,273,345 | 13,076,037 |
| model_sql_change | 3 | 0 | 0 | 0 | 0 | 0 | 3 | - | - | - | - |

## kingyo-prototype-columns (changed-column hints)

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| unchanged_no_signal | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.000 | 0.01 | 3,222,061 | 0 |
| unchanged_resignal | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.197 | 0.73 | 3,222,061 | 635,530 |
| new_day | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.204 | 0.63 | 3,227,461 | 659,441 |
| backfill_7_days | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.204 | 0.38 | 3,227,416 | 659,305 |
| late_updates_recent | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.234 | 0.58 | 3,222,061 | 754,488 |
| late_updates_scattered | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 2.306 | 2.01 | 3,222,061 | 7,429,649 |
| dimension_change | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 1.220 | 1.61 | 3,222,061 | 3,930,213 |
| source_column_added | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0.000 | 0.05 | 3,222,061 | 6 |
| edge_mix | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 3.993 | 1.33 | 3,274,703 | 13,078,527 |
| model_sql_change | 3 | 0 | 0 | 0 | 0 | 0 | 3 | - | - | - | - |

Seeds only change which rows are picked, so ratios are nearly identical across seeds.

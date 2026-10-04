# Fact-source partitioning layouts, SF 0.05, 2 seeds, scan ratio vs full rebuild (DuckDB zone-map proxy)

## layout=order_date candidate=kingyo-prototype

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| unchanged_resignal | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0.198 | 0.73 | 3,201,581 | 633,542 |
| new_day | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0.206 | 0.61 | 3,206,981 | 661,842 |
| late_updates_recent | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0.237 | 0.74 | 3,201,581 | 757,312 |
| late_updates_scattered | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 6.387 | 1.59 | 3,201,581 | 20,451,094 |
| edge_mix | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 3.459 | 1.29 | 3,249,789 | 11,245,607 |

## layout=order_date candidate=kingyo-prototype-columns

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| unchanged_resignal | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0.198 | 0.46 | 3,201,581 | 633,542 |
| new_day | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0.206 | 0.62 | 3,206,981 | 661,842 |
| late_updates_recent | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0.234 | 0.95 | 3,201,581 | 747,899 |
| late_updates_scattered | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 6.083 | 1.51 | 3,201,581 | 19,476,789 |
| edge_mix | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 3.450 | 0.68 | 3,253,878 | 11,230,807 |

## layout=unpartitioned candidate=kingyo-prototype

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| unchanged_resignal | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 1.34 | 4,814,157 | 9,630,018 |
| new_day | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 1.86 | 4,823,757 | 9,649,218 |
| late_updates_recent | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 1.28 | 4,814,157 | 9,630,018 |
| late_updates_scattered | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 1.40 | 4,814,157 | 9,630,018 |
| edge_mix | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 1.12 | 4,814,281 | 9,630,266 |

## layout=unpartitioned candidate=kingyo-prototype-columns

| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| unchanged_resignal | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 1.14 | 4,814,157 | 9,630,018 |
| new_day | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.001 | 2.04 | 4,823,333 | 9,649,218 |
| late_updates_recent | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 1.626 | 1.41 | 4,814,157 | 7,829,697 |
| late_updates_scattered | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 1.626 | 1.53 | 4,814,157 | 7,829,697 |
| edge_mix | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 2.000 | 0.85 | 4,814,274 | 9,630,252 |

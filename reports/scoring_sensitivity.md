# Scoring Sensitivity Discussion

Baseline governance weights were stress-tested against three alternative weighting schemes.

| Scenario | Top-100 Overlap vs Baseline | Jaccard | Avg Rank Shift (within baseline top-100) |
|---|---:|---:|---:|
| churn_heavy | 86.0% | 0.754 | 11.63 |
| discount_heavy | 88.0% | 0.786 | 10.96 |
| exposure_heavy | 84.0% | 0.724 | 24.93 |

Interpretation:
- High overlap indicates shortlist stability under reasonable policy-weight shifts.
- Rank shifts highlight accounts sensitive to policy emphasis (for example discount-heavy vs exposure-heavy governance).
- Baseline weights are suitable as an operating default; scenario views should be used in planning cycles and policy reviews.
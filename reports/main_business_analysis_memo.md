# Main Business Analysis Memo

## Scope and Definitions
Analysis window: 2023-03-01 to 2026-02-01 (36 months).

Core metric definitions:
- `MRR`: sum of `active_mrr` in month.
- `ARR`: `12 * MRR`.
- `GRR`: `(starting_mrr - contraction_mrr - churn_mrr) / starting_mrr`.
- `NRR`: `(starting_mrr + expansion_mrr - contraction_mrr - churn_mrr) / starting_mrr`.
- `Logo churn rate`: churn events / active account-month rows.
- `Revenue churn rate`: churned MRR / active MRR.

## 1) Revenue Quality Overview
**Key takeaway:** Strong topline growth with non-trivial quality exposure in discounted and at-risk revenue pockets.

- MRR: `$3,749,108` -> `$9,523,590` (`2.70%` implied monthly growth).
- ARR run-rate: `$44,989,299` -> `$114,283,079`.
- Latest weighted realized price index: `0.822`.
- Latest weighted discount: `17.7%`.
- Latest discounted-dependent MRR share: `15.9%`.
- Latest high-risk MRR share: `4.0%`.

Interpretation:
- Growth quality improved on pricing realization, but downside concentration remains material.

Caveat:
- Realized pricing reflects both commercial pricing and collections quality.

## 2) Retention and Churn Diagnostics
**Key takeaway:** Portfolio retention is stable but expansion buffer remains thin.

- Logo churn: `0.82%`.
- Revenue churn: `0.58%`.
- Latest GRR/NRR: `99.17%` / `99.83%`.

Interpretation:
- NRR near parity indicates limited cushion if churn or contraction rises.

Caveat:
- Diagnostics are associative; they do not infer causal channel or segment effects.

## 3) Discount and Fragility
**Key takeaway:** Higher discount intensity is associated with higher forward churn in the highest discount bands.

- Worst discount-band forward churn (3m): `>30%` at `4.31%`.

Interpretation:
- Extreme discounting should be treated as a governance signal, especially near renewal.

Caveat:
- Correlation does not establish causality.

## 4) Expansion Quality
**Key takeaway:** Expansion remains positive but fragile expansion share is material.

- Fragile expansion MRR share: `28.0%`.
- Total expansion MRR observed: `$1,631,578`.

Interpretation:
- Part of growth is potentially less durable and should be monitored post-expansion.

## 5) Account Health and Risk Concentration
**Key takeaway:** Downside risk is concentrated enough to prioritize with account-level governance.

- At-risk accounts: `80`.
- At-risk MRR: `$380,663`.
- Top-20 share within at-risk MRR: `80.5%`.

## Final Synthesis
- Healthy: strong recurring scale-up and stable gross retention.
- Fragile: meaningful discounted and high-risk revenue exposure.
- Biggest risk: concentrated downside in a small set of accounts with weak forward signals.
- Leadership blind spot if focused only on topline: growth durability can deteriorate before headline MRR does.

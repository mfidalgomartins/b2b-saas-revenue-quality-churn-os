# Main Business Analysis

Window: 2023-03-01 to 2026-02-01 (36 months).
All findings are associative; correlation does not establish causality.

## Definitions

- MRR — sum of `active_mrr` in month.
- ARR — 12 × MRR.
- GRR — `(starting_mrr − contraction_mrr − churn_mrr) / starting_mrr`.
- NRR — `(starting_mrr + expansion_mrr − contraction_mrr − churn_mrr) / starting_mrr`.
- Logo churn rate — churned logos / beginning-of-month active logos.
- Revenue churn rate — churned MRR / beginning MRR.

## Revenue quality

MRR $3,749,108 → $9,523,590
(2.70% implied monthly growth); ARR run-rate
$44,989,299 → $114,283,079.

Latest weighted realized price index 0.822, latest
weighted discount 17.7%. Discount-dependent MRR share
15.9%; High/Critical-priority MRR share
4.0%.

Realized pricing mixes commercial discount and collection-loss effects and is
not a clean pricing metric on its own.

## Retention and churn

- Latest logo churn — 0.73%
- Latest revenue churn — 0.39%
- Latest GRR / NRR — 99.17% / 99.82%

NRR near parity leaves little buffer if churn or contraction accelerates.

## Discount and fragility

Worst discount band on forward 3-month churn: **>30%** at
4.31%. This makes discount intensity near renewal a useful
prioritisation signal in the simulated panel.

## Expansion quality

Fragile expansion share 28.0% of $1,631,578 expansion
MRR in window. Fragile expansion is correlated with elevated churn 3–9 months
later in the simulated panel.

## Account-level concentration

- High/Critical-priority accounts — 79
- At-risk MRR — $376,193
- Top-20 share inside at-risk MRR — 81.3%

A small group of accounts carries most of the downside. Account-level
prioritisation should complement portfolio-wide commercial policy.

## What leadership should watch

Topline MRR is not a sufficient health signal: growth durability degrades in
discount intensity, expansion quality, and concentration metrics before it
shows up in headline retention.

## Caveats

- Realized price index mixes commercial discount and collection-loss effects.
- All retention diagnostics are associative; no causal channel or segment
  effect is inferred. Correlation does not establish causation.
- Trailing-feature windows use last-active observations and are not strictly
  contiguous for accounts that churned and returned (no such cases here).
- Concentration weights are sensitive to current-snapshot timing.

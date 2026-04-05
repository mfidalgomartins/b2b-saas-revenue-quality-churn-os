# Data Profiling Memo

## Executive Summary
- Coverage window: `2021-07-01` to `2026-02-28`
- Tables profiled: `6`
- Material issues identified: `0` (High/Critical: `0`)
- Assessment: Ready for analysis

## Table Inventory
- `customers`: 4,500 rows, 9 columns, PK candidate `['customer_id']`
- `plans`: 8 rows, 6 columns, PK candidate `['plan_id']`
- `subscriptions`: 99,729 rows, 11 columns, PK candidate `['subscription_id']`
- `monthly_account_metrics`: 112,038 rows, 13 columns, PK candidate `['customer_id', 'month']`
- `invoices`: 99,729 rows, 10 columns, PK candidate `['invoice_id']`
- `account_managers`: 40 rows, 4 columns, PK candidate `['account_manager_id']`

## Key Integrity Checks
- Referential integrity violations: `0`
- Subscription date coherence issues: `0`
- Signup chronology issues: `0`
- Invoice effective-adjustment mismatches (>2 cents): `0`
- Churn flag/status misalignment rows: `0`
- Future-month misalignment flags: `0`

## Issues Ranked by Severity
No material data quality issues detected in current run.

## Analytical Implications
- Revenue quality and retention conclusions are reliable only when invoice arithmetic and churn/status coherence are preserved.
- Segment/channel diagnostics depend on zero RI breaks; otherwise, denominator inflation risk emerges.
- Temporal consistency is a hard precondition for churn early-warning credibility.

## Recommended Focus Areas for Main Analysis
1. Discount discipline versus realized pricing quality (separating commercial discount from collection loss).
2. Renewal-window churn concentration by segment/channel.
3. Fragile expansion identification (growth with deteriorating health signals).
4. Concentration-adjusted downside exposure in High/Critical governance tiers.

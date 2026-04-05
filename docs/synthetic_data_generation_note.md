# Synthetic Data Generation Note

## Scope
- Customers: 4,500
- History length: 36 monthly periods ending 2026-02-01
- Subscription-month snapshots: 99,729
- Monthly account metric rows: 112,038
- Invoice rows: 99,729

## Embedded Business Logic
- Segment-specific retention behavior: Enterprise has lower baseline churn than SMB.
- Discount behavior varies by acquisition channel, billing cycle, and customer quality.
- Churn probability increases when usage declines, NPS falls, payment delays rise, support burden rises, and discounting is heavy.
- Healthy expansions happen for high-usage/high-NPS/low-delay accounts.
- Fragile expansion path is explicitly simulated: some accounts expand under deep discounts then face elevated churn risk 3-9 months later.
- Hidden risk accounts are simulated with high current MRR but degrading leading indicators.
- Revenue concentration is introduced via a small set of high-seat enterprise/concentrated accounts.
- Renewal seasonality is encoded through renewal probabilities and churn pressure around renewal windows.

## Quick Diagnostics
- Unique churned customers in window: 822
- Average discount_pct in subscriptions: 18.24%
- Top-10 account concentration (share of peak contracted MRR): 3.61%
- Payment status mix: {'paid_late': 0.736, 'paid_on_time': 0.26, 'overdue': 0.004}

## Intended Patterns for Downstream Analysis
- Better GRR/NRR in enterprise cohorts vs SMB cohorts.
- Higher discount intensity in paid_media and outbound-led acquisition.
- At-risk ARR concentrated where usage/NPS trend down and delays/support worsen.
- Expansion quality split: healthy expansion cohorts retain better than high-discount expansion cohorts.
- High-MRR hidden-risk account watchlist should surface when combining ARR exposure with forward risk.

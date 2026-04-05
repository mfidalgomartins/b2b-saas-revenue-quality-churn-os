# Analytical Layer Purpose and Design Notes

## Why each analytical table exists
- `account_monthly_revenue_quality`: monthly revenue quality lens at account grain for trend diagnostics, retention decomposition, and dashboard time-series.
- `customer_health_features`: latest account feature vector for churn/risk scoring and account prioritization.
- `cohort_retention_summary`: standardized GRR/NRR cohort tracking by segment and region.
- `account_risk_base`: scoring-ready payload with auditable inputs and forward risk flags.
- `account_manager_summary`: portfolio governance layer for frontline ownership performance and risk concentration.

## Leakage controls applied
- Trailing feature windows use data up to snapshot month only.
- Cohort metrics are computed from month-index progression without future-period backfill.
- Risk inputs are assembled from current/trailing states only, with no forward outcome labels embedded.

## Reproducibility
- One deterministic script builds all outputs from `data/raw` and writes to `data/processed`.
- All rule thresholds are explicit in code and can be versioned.

## Traceability
- Engineered fields are direct transforms from raw columns with documented formulas in the feature dictionary.

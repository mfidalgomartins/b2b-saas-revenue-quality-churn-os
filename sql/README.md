# SQL Semantic Layer (Reference)

This folder provides SQL equivalents for key staging and mart transformations used in the Python pipeline.

## Purpose
- Make warehouse migration and metric review straightforward.
- Provide a migration path to warehouse-native modeling (dbt/DuckDB/Snowflake/BigQuery).
- Keep business metric definitions explicit and reviewable in SQL.

## Structure
- `staging/`: typed source-cleaning views.
- `marts/`: business-facing models for KPI consumption.

## Notes
- SQL models are reference implementations and should be adapted to your target warehouse SQL dialect.
- The project's authoritative artifact build remains Python-based for reproducibility in this repository.
- `tests/test_sql_python_parity.py` executes `stg_monthly_account_metrics`, `stg_subscriptions`,
  `stg_invoices`, `mart_account_monthly_revenue_quality`, and `mart_retention_monthly` against the raw CSVs
  via DuckDB and asserts the retention mart (starting MRR, GRR, NRR, logo/revenue churn) matches
  `src/metrics.build_monthly_retention` to 1e-6 — proof the SQL mirror is correct, not just present.
  Scope note: this covers the retention mart; `mart_account_monthly_revenue_quality`'s own
  `revenue_quality_flag`/`discount_dependency_flag` columns are exercised as a dependency but not yet
  independently asserted against the Python `revenue_quality_flag` logic — a natural next parity target.

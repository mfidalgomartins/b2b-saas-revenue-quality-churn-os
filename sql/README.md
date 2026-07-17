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
- `tests/test_sql_python_parity.py` executes every staging and mart model in DuckDB and compares the complete
  outputs with their Python counterparts. Account-month revenue-quality and retention metrics use a 1e-6
  numeric tolerance and exact categorical equality. Account scoring covers all five scores and tiers; score
  tolerance is one 0.001 output-precision unit because component CSV serialization occurs before DuckDB
  recomposition, while tiers must match exactly.
- `mart_account_scoring` starts from the persisted weighted contribution table. That table includes only the
  minimal policy-state fields required to reproduce inactive-account, no-expansion and escalation overrides;
  raw feature derivation remains upstream in the shared Python scoring component library.

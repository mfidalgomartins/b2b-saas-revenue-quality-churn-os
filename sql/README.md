# SQL Semantic Layer (Reference)

This folder provides SQL equivalents for key staging and mart transformations used in the Python pipeline.

## Purpose
- Improve analytics-engineering interview defensibility.
- Provide a migration path to warehouse-native modeling (dbt/DuckDB/Snowflake/BigQuery).
- Keep business metric definitions explicit and reviewable in SQL.

## Structure
- `staging/`: typed source-cleaning views.
- `marts/`: business-facing models for KPI consumption.

## Notes
- SQL models are reference implementations and should be adapted to your target warehouse SQL dialect.
- The project’s authoritative artifact build remains Python-based for reproducibility in this repository.
- Parity checks between SQL and Python outputs should be added if moving to SQL-first production execution.

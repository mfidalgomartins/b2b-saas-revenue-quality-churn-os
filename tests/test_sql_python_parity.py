"""Parity check: sql/marts/*.sql (run via DuckDB) vs. the Python metrics layer.

sql/README.md documents the SQL folder as a reference implementation and
explicitly flags that "parity checks ... should be added if moving to
SQL-first production execution." This test is that check for the retention
mart: it executes the staging + mart SQL directly against the raw CSVs with
DuckDB, and asserts the result matches src.metrics.build_monthly_retention
(the function that actually powers the report, dashboard, and validation
gate) to a tight numeric tolerance.

Requires data/raw/*.csv and data/processed/account_monthly_revenue_quality.csv
to exist locally — run `make data` (or the full pipeline) first if this skips.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.metrics import build_monthly_retention  # noqa: E402

DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
SQL_STAGING = ROOT / "sql" / "staging"
SQL_MARTS = ROOT / "sql" / "marts"


class TestSqlPythonRetentionParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        required = [
            DATA_RAW / "customers.csv",
            DATA_RAW / "subscriptions.csv",
            DATA_RAW / "monthly_account_metrics.csv",
            DATA_RAW / "invoices.csv",
            DATA_PROCESSED / "account_monthly_revenue_quality.csv",
        ]
        missing = [str(p) for p in required if not p.exists()]
        if missing:
            raise unittest.SkipTest(f"Required data files missing (run `make data`/pipeline first): {missing}")

        try:
            import duckdb
        except ImportError:
            raise unittest.SkipTest("duckdb not installed (dev dependency)") from None

        cls.con = duckdb.connect(":memory:")
        for table in ("customers", "subscriptions", "monthly_account_metrics", "invoices"):
            cls.con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto(?)", [str(DATA_RAW / f"{table}.csv")])

        for view in ("stg_monthly_account_metrics", "stg_subscriptions", "stg_invoices"):
            sql_text = (SQL_STAGING / f"{view}.sql").read_text(encoding="utf-8")
            cls.con.execute(f"CREATE VIEW {view} AS {sql_text}")

        for view in ("mart_account_monthly_revenue_quality", "mart_retention_monthly"):
            sql_text = (SQL_MARTS / f"{view}.sql").read_text(encoding="utf-8")
            cls.con.execute(f"CREATE VIEW {view} AS {sql_text}")

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "con"):
            cls.con.close()

    def test_retention_mart_matches_python_metrics_layer(self) -> None:
        sql_result = self.con.execute("SELECT * FROM mart_retention_monthly ORDER BY month").fetchdf()
        sql_result["month"] = pd.to_datetime(sql_result["month"])

        quality = pd.read_csv(DATA_PROCESSED / "account_monthly_revenue_quality.csv", parse_dates=["month"])
        metrics = pd.read_csv(DATA_RAW / "monthly_account_metrics.csv", parse_dates=["month"])
        python_result = build_monthly_retention(quality, metrics).sort_values("month").reset_index(drop=True)

        merged = sql_result.merge(python_result, on="month", how="inner", suffixes=("_sql", "_py"))
        self.assertEqual(
            len(merged),
            len(python_result),
            "SQL and Python retention results cover a different set of months",
        )

        column_pairs = [
            ("starting_mrr", "starting_mrr"),
            ("expansion_mrr", "expansion_mrr"),
            ("contraction_mrr", "contraction_mrr"),
            ("churn_mrr", "churn_mrr"),
            ("starting_logos", "starting_logos"),
            ("churned_logos", "churn_events"),
            ("logo_churn_rate", "logo_churn_rate"),
            ("revenue_churn_rate", "revenue_churn_rate"),
            ("gross_revenue_retention", "grr"),
            ("net_revenue_retention", "nrr"),
        ]
        for sql_col, py_col in column_pairs:
            sql_series = merged[f"{sql_col}_sql" if sql_col in python_result.columns else sql_col].astype(float)
            py_series = merged[f"{py_col}_py" if py_col in sql_result.columns else py_col].astype(float)
            max_abs_diff = (sql_series - py_series).abs().max()
            self.assertLess(
                max_abs_diff,
                1e-6,
                f"SQL '{sql_col}' vs Python '{py_col}' diverge by up to {max_abs_diff}",
            )


if __name__ == "__main__":
    unittest.main()

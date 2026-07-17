"""DuckDB parity checks for every reference SQL mart.

The tests execute staging and mart SQL directly against canonical CSVs, then
compare account revenue quality, retention and score composition with the
Python artifacts that power the report and governance gate.

Requires a built local pipeline — run `make all` first if this skips.
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


class TestSqlPythonParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        required = [
            DATA_RAW / "customers.csv",
            DATA_RAW / "subscriptions.csv",
            DATA_RAW / "monthly_account_metrics.csv",
            DATA_RAW / "invoices.csv",
            DATA_PROCESSED / "account_monthly_revenue_quality.csv",
            DATA_PROCESSED / "account_scoring_components.csv",
            DATA_PROCESSED / "account_scoring_model_output.csv",
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
        cls.con.execute(
            "CREATE TABLE account_scoring_components AS SELECT * FROM read_csv_auto(?)",
            [str(DATA_PROCESSED / "account_scoring_components.csv")],
        )

        for view in ("stg_monthly_account_metrics", "stg_subscriptions", "stg_invoices"):
            sql_text = (SQL_STAGING / f"{view}.sql").read_text(encoding="utf-8")
            cls.con.execute(f"CREATE VIEW {view} AS {sql_text}")

        for view in ("mart_account_monthly_revenue_quality", "mart_retention_monthly", "mart_account_scoring"):
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

    def test_account_monthly_revenue_quality_matches_python_mart(self) -> None:
        sql_result = self.con.execute(
            "SELECT * FROM mart_account_monthly_revenue_quality ORDER BY customer_id, month"
        ).fetchdf()
        sql_result["month"] = pd.to_datetime(sql_result["month"])
        python_result = (
            pd.read_csv(DATA_PROCESSED / "account_monthly_revenue_quality.csv", parse_dates=["month"])
            .sort_values(["customer_id", "month"])
            .reset_index(drop=True)
        )
        merged = sql_result.merge(
            python_result,
            on=["customer_id", "month"],
            how="inner",
            suffixes=("_sql", "_py"),
            validate="one_to_one",
        )
        self.assertEqual(len(merged), len(python_result), "SQL and Python account-month key universes differ")

        numeric_columns = [
            "active_mrr",
            "realized_price_index",
            "avg_discount_pct",
            "expansion_mrr",
            "contraction_mrr",
            "net_mrr_change",
            "discount_dependency_flag",
            "renewal_risk_proxy",
        ]
        for column in numeric_columns:
            max_abs_diff = (merged[f"{column}_sql"] - merged[f"{column}_py"]).abs().max()
            self.assertLess(max_abs_diff, 1e-6, f"Account-month column '{column}' differs by {max_abs_diff}")
        self.assertTrue(
            merged["revenue_quality_flag_sql"].eq(merged["revenue_quality_flag_py"]).all(),
            "SQL and Python revenue_quality_flag values differ",
        )

    def test_account_scoring_mart_matches_python_scores_and_tiers(self) -> None:
        sql_result = self.con.execute("SELECT * FROM mart_account_scoring ORDER BY customer_id").fetchdf()
        python_result = (
            pd.read_csv(DATA_PROCESSED / "account_scoring_model_output.csv")
            .sort_values("customer_id")
            .reset_index(drop=True)
        )
        merged = sql_result.merge(
            python_result,
            on="customer_id",
            how="inner",
            suffixes=("_sql", "_py"),
            validate="one_to_one",
        )
        self.assertEqual(len(merged), len(python_result), "SQL and Python scoring customer universes differ")

        score_columns = [
            "churn_risk_score",
            "revenue_quality_score",
            "discount_dependency_score",
            "expansion_quality_score",
            "governance_priority_score",
        ]
        tier_columns = [
            "churn_risk_tier",
            "revenue_quality_risk_tier",
            "discount_dependency_tier",
            "expansion_quality_risk_tier",
            "governance_priority_tier",
        ]
        for column in score_columns:
            max_abs_diff = (merged[f"{column}_sql"] - merged[f"{column}_py"]).abs().max()
            self.assertLessEqual(
                max_abs_diff,
                0.0010001,
                f"Scoring column '{column}' differs by more than one output precision unit: {max_abs_diff}",
            )
        for column in tier_columns:
            self.assertTrue(
                merged[f"{column}_sql"].eq(merged[f"{column}_py"]).all(),
                f"SQL and Python tier '{column}' values differ",
            )


if __name__ == "__main__":
    unittest.main()

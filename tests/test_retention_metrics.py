from __future__ import annotations

import unittest

import pandas as pd

from src.metrics import build_monthly_retention, build_retention_panel


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    quality = pd.DataFrame(
        [
            {
                "customer_id": "legacy",
                "month": "2024-01-01",
                "active_mrr": 100.0,
                "expansion_mrr": 0.0,
                "contraction_mrr": 0.0,
            },
            {
                "customer_id": "legacy",
                "month": "2024-02-01",
                "active_mrr": 90.0,
                "expansion_mrr": 0.0,
                "contraction_mrr": 10.0,
            },
            {
                "customer_id": "new",
                "month": "2024-02-01",
                "active_mrr": 50.0,
                "expansion_mrr": 0.0,
                "contraction_mrr": 0.0,
            },
            {
                "customer_id": "legacy",
                "month": "2024-03-01",
                "active_mrr": 100.0,
                "expansion_mrr": 10.0,
                "contraction_mrr": 0.0,
            },
            {
                "customer_id": "new",
                "month": "2024-03-01",
                "active_mrr": 50.0,
                "expansion_mrr": 0.0,
                "contraction_mrr": 0.0,
            },
        ]
    )
    metrics = pd.DataFrame(
        [
            {"customer_id": "legacy", "month": "2024-01-01", "active_flag": 1, "churn_flag": 0},
            {"customer_id": "legacy", "month": "2024-02-01", "active_flag": 1, "churn_flag": 0},
            {"customer_id": "new", "month": "2024-02-01", "active_flag": 1, "churn_flag": 0},
            {"customer_id": "legacy", "month": "2024-03-01", "active_flag": 1, "churn_flag": 0},
            {"customer_id": "new", "month": "2024-03-01", "active_flag": 1, "churn_flag": 1},
        ]
    )
    for frame in (quality, metrics):
        frame["month"] = pd.to_datetime(frame["month"])
    return quality, metrics


class TestRetentionMetrics(unittest.TestCase):
    def test_new_logo_is_excluded_from_retention_base(self) -> None:
        quality, metrics = _frames()
        panel = build_retention_panel(quality, metrics)
        new_logo = panel[(panel["customer_id"] == "new") & (panel["month"] == pd.Timestamp("2024-02-01"))].iloc[0]

        self.assertTrue(new_logo["is_new_logo"])
        self.assertFalse(new_logo["retention_eligible"])
        self.assertEqual(float(new_logo["starting_mrr"]), 0.0)

    def test_monthly_bridge_uses_beginning_mrr(self) -> None:
        quality, metrics = _frames()
        monthly = build_monthly_retention(quality, metrics).set_index("month")

        feb = monthly.loc[pd.Timestamp("2024-02-01")]
        self.assertEqual(float(feb["starting_mrr"]), 100.0)
        self.assertEqual(int(feb["starting_logos"]), 1)
        self.assertEqual(int(feb["new_logos"]), 1)
        self.assertAlmostEqual(float(feb["grr"]), 0.90)
        self.assertAlmostEqual(float(feb["nrr"]), 0.90)

        mar = monthly.loc[pd.Timestamp("2024-03-01")]
        self.assertEqual(float(mar["starting_mrr"]), 140.0)
        self.assertEqual(float(mar["churn_mrr"]), 50.0)
        self.assertAlmostEqual(float(mar["logo_churn_rate"]), 0.50)
        self.assertAlmostEqual(float(mar["grr"]), 90.0 / 140.0)
        self.assertAlmostEqual(float(mar["nrr"]), 100.0 / 140.0)


class TestRetentionGuards(unittest.TestCase):
    def test_missing_quality_columns_raise(self) -> None:
        _, metrics = _frames()
        bad_quality = pd.DataFrame({"customer_id": ["legacy"], "month": [pd.Timestamp("2024-01-01")]})
        with self.assertRaises(ValueError) as ctx:
            build_retention_panel(bad_quality, metrics)
        self.assertIn("required columns", str(ctx.exception))

    def test_mismatched_key_universe_raises(self) -> None:
        quality, metrics = _frames()
        # Drop a customer-month from metrics so the left join leaves NaN flags.
        metrics = metrics.iloc[1:].reset_index(drop=True)
        with self.assertRaises(ValueError) as ctx:
            build_retention_panel(quality, metrics)
        self.assertIn("same customer-month key universe", str(ctx.exception))

    def test_zero_starting_base_yields_nan_ratios(self) -> None:
        # A month whose only active account is a brand-new logo has no retention
        # base, so GRR/NRR/logo-churn must be NaN rather than a divide-by-zero.
        quality = pd.DataFrame(
            [
                {
                    "customer_id": "a",
                    "month": "2024-01-01",
                    "active_mrr": 100.0,
                    "expansion_mrr": 0.0,
                    "contraction_mrr": 0.0,
                },
                {
                    "customer_id": "b",
                    "month": "2024-02-01",
                    "active_mrr": 50.0,
                    "expansion_mrr": 0.0,
                    "contraction_mrr": 0.0,
                },
            ]
        )
        metrics = pd.DataFrame(
            [
                {"customer_id": "a", "month": "2024-01-01", "active_flag": 1, "churn_flag": 0},
                {"customer_id": "b", "month": "2024-02-01", "active_flag": 1, "churn_flag": 0},
            ]
        )
        for frame in (quality, metrics):
            frame["month"] = pd.to_datetime(frame["month"])
        monthly = build_monthly_retention(quality, metrics).set_index("month")
        feb = monthly.loc[pd.Timestamp("2024-02-01")]
        self.assertEqual(float(feb["starting_mrr"]), 0.0)
        self.assertTrue(pd.isna(feb["grr"]))
        self.assertTrue(pd.isna(feb["nrr"]))
        self.assertTrue(pd.isna(feb["logo_churn_rate"]))


if __name__ == "__main__":
    unittest.main()

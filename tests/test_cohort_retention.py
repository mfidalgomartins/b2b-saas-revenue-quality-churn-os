"""Unit tests for cohort retention computation.

The retention table is the backbone of every GRR/NRR figure in the executive
dashboard. These tests pin the formula so that any regression in cohort
indexing, base-revenue capping, or rate computation surfaces immediately.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.features.build_analytical_layer import build_cohort_retention_summary  # noqa: E402


def _make_tables(customers, subs, amrq):
    return {
        "customers": pd.DataFrame(customers),
        "subscriptions": pd.DataFrame(subs),
    }, pd.DataFrame(amrq)


class TestCohortRetention(unittest.TestCase):
    def test_full_retention_returns_one(self) -> None:
        tables, amrq = _make_tables(
            customers=[{"customer_id": "C1", "segment": "SMB", "region": "EU"}],
            subs=[{"customer_id": "C1", "subscription_start_date": pd.Timestamp("2024-01-01")}],
            amrq=[
                {"customer_id": "C1", "month": pd.Timestamp("2024-01-01"), "active_mrr": 1000.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-02-01"), "active_mrr": 1000.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-03-01"), "active_mrr": 1000.0},
            ],
        )
        out = build_cohort_retention_summary(tables, amrq)
        for _, row in out.iterrows():
            self.assertAlmostEqual(row["gross_retention_rate"], 1.0, places=6)
            self.assertAlmostEqual(row["net_retention_rate"], 1.0, places=6)

    def test_churn_drops_grr_to_zero(self) -> None:
        tables, amrq = _make_tables(
            customers=[{"customer_id": "C1", "segment": "SMB", "region": "EU"}],
            subs=[{"customer_id": "C1", "subscription_start_date": pd.Timestamp("2024-01-01")}],
            amrq=[
                {"customer_id": "C1", "month": pd.Timestamp("2024-01-01"), "active_mrr": 1000.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-02-01"), "active_mrr": 0.0},
            ],
        )
        out = build_cohort_retention_summary(tables, amrq)
        m1 = out[out["month_number"] == 1].iloc[0]
        self.assertAlmostEqual(m1["gross_retention_rate"], 0.0, places=6)
        self.assertAlmostEqual(m1["net_retention_rate"], 0.0, places=6)

    def test_expansion_lifts_nrr_but_not_grr(self) -> None:
        # Expansion above starting base must increase NRR while GRR stays capped at 1.
        tables, amrq = _make_tables(
            customers=[{"customer_id": "C1", "segment": "MID", "region": "NA"}],
            subs=[{"customer_id": "C1", "subscription_start_date": pd.Timestamp("2024-01-01")}],
            amrq=[
                {"customer_id": "C1", "month": pd.Timestamp("2024-01-01"), "active_mrr": 1000.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-02-01"), "active_mrr": 1500.0},
            ],
        )
        out = build_cohort_retention_summary(tables, amrq)
        m1 = out[out["month_number"] == 1].iloc[0]
        self.assertAlmostEqual(m1["gross_retention_rate"], 1.0, places=6)
        self.assertAlmostEqual(m1["net_retention_rate"], 1.5, places=6)

    def test_contraction_reduces_grr_proportionally(self) -> None:
        tables, amrq = _make_tables(
            customers=[{"customer_id": "C1", "segment": "ENT", "region": "EU"}],
            subs=[{"customer_id": "C1", "subscription_start_date": pd.Timestamp("2024-01-01")}],
            amrq=[
                {"customer_id": "C1", "month": pd.Timestamp("2024-01-01"), "active_mrr": 1000.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-02-01"), "active_mrr": 600.0},
            ],
        )
        out = build_cohort_retention_summary(tables, amrq)
        m1 = out[out["month_number"] == 1].iloc[0]
        self.assertAlmostEqual(m1["gross_retention_rate"], 0.6, places=6)
        self.assertAlmostEqual(m1["net_retention_rate"], 0.6, places=6)

    def test_cohort_indexing_starts_at_zero(self) -> None:
        tables, amrq = _make_tables(
            customers=[{"customer_id": "C1", "segment": "SMB", "region": "EU"}],
            subs=[{"customer_id": "C1", "subscription_start_date": pd.Timestamp("2024-03-01")}],
            amrq=[
                {"customer_id": "C1", "month": pd.Timestamp("2024-03-01"), "active_mrr": 500.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-04-01"), "active_mrr": 500.0},
            ],
        )
        out = build_cohort_retention_summary(tables, amrq)
        self.assertEqual(out["month_number"].min(), 0)
        self.assertEqual(out["month_number"].max(), 1)

    def test_pre_cohort_months_are_excluded(self) -> None:
        # An active_mrr row before the cohort_month must not appear in output.
        tables, amrq = _make_tables(
            customers=[{"customer_id": "C1", "segment": "SMB", "region": "EU"}],
            subs=[{"customer_id": "C1", "subscription_start_date": pd.Timestamp("2024-03-01")}],
            amrq=[
                {"customer_id": "C1", "month": pd.Timestamp("2024-01-01"), "active_mrr": 1.0},
                {"customer_id": "C1", "month": pd.Timestamp("2024-03-01"), "active_mrr": 500.0},
            ],
        )
        out = build_cohort_retention_summary(tables, amrq)
        self.assertTrue((out["month_number"] >= 0).all())


if __name__ == "__main__":
    unittest.main()

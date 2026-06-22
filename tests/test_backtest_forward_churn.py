"""Equivalence test for the vectorised forward-churn labeller.

The backtest performance work replaced a per-row ``DateOffset`` loop with a
``searchsorted`` over a churn prefix-sum. This pins the new implementation to the
original date-window semantics, including customers with gaps in their months.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.scoring.backtest_scoring_calibration import attach_forward_churn  # noqa: E402


def _reference(panel: pd.DataFrame, horizon_months: int) -> pd.DataFrame:
    """The original O(rows) date-offset implementation, kept here as the oracle."""
    panel = panel.sort_values(["customer_id", "month"]).reset_index(drop=True)
    out = panel[["customer_id", "month", "churn_flag"]].copy()
    out["forward_churn_flag"] = 0
    for _, group_idx in panel.groupby("customer_id", sort=False).groups.items():
        rows = panel.loc[group_idx, ["month", "churn_flag"]].sort_values("month")
        months = rows["month"].to_numpy()
        churn = rows["churn_flag"].fillna(0).astype(int).to_numpy()
        flags = np.zeros(len(rows), dtype=int)
        for i in range(len(rows)):
            upper = pd.Timestamp(months[i]) + pd.DateOffset(months=horizon_months)
            mask = (months > months[i]) & (months <= upper)
            flags[i] = int(churn[mask].any()) if mask.any() else 0
        out.loc[rows.index, "forward_churn_flag"] = flags
    return panel.merge(
        out[["customer_id", "month", "forward_churn_flag"]],
        on=["customer_id", "month"],
        how="left",
    )


def _synthetic_panel(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for cust in range(40):
        months = pd.date_range("2024-01-01", periods=18, freq="MS")
        # Randomly drop some months to create gaps.
        keep = rng.random(len(months)) > 0.2
        for m in months[keep]:
            rows.append(
                {
                    "customer_id": f"c{cust}",
                    "month": m,
                    "churn_flag": int(rng.random() < 0.12),
                }
            )
    return pd.DataFrame(rows)


class TestAttachForwardChurn(unittest.TestCase):
    def test_matches_reference_with_gaps(self) -> None:
        for horizon in (1, 3, 6):
            for seed in range(5):
                panel = _synthetic_panel(seed)
                got = (
                    attach_forward_churn(panel.copy(), horizon)
                    .sort_values(["customer_id", "month"])["forward_churn_flag"]
                    .to_numpy()
                )
                want = (
                    _reference(panel.copy(), horizon)
                    .sort_values(["customer_id", "month"])["forward_churn_flag"]
                    .to_numpy()
                )
                with self.subTest(horizon=horizon, seed=seed):
                    np.testing.assert_array_equal(got, want)

    def test_horizon_window_is_inclusive_upper_exclusive_current(self) -> None:
        panel = pd.DataFrame(
            [
                {"customer_id": "a", "month": pd.Timestamp("2024-01-01"), "churn_flag": 0},
                {"customer_id": "a", "month": pd.Timestamp("2024-03-01"), "churn_flag": 1},
            ]
        )
        # Jan + 2 months = Mar, so horizon 2 includes the Mar churn; horizon 1 does not.
        h2 = attach_forward_churn(panel.copy(), 2).set_index("month")["forward_churn_flag"]
        h1 = attach_forward_churn(panel.copy(), 1).set_index("month")["forward_churn_flag"]
        self.assertEqual(int(h2.loc[pd.Timestamp("2024-01-01")]), 1)
        self.assertEqual(int(h1.loc[pd.Timestamp("2024-01-01")]), 0)


if __name__ == "__main__":
    unittest.main()

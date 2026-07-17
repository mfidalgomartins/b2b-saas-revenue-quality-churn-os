"""Tests for randomized intervention assignment and ITT measurement."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from src.interventions.assignment import AssignmentConfig, build_randomized_assignment, validate_assignment_ledger
from src.interventions.effectiveness import EffectivenessConfig, attach_forward_outcomes, evaluate_intervention


def _account_history() -> tuple[pd.DataFrame, pd.DataFrame]:
    customers = pd.DataFrame(
        {
            "customer_id": [f"C{index:03d}" for index in range(80)],
            "segment": np.repeat(["SMB", "Mid-Market"], 40),
            "account_manager_id": np.repeat(["AM1", "AM2"], 40),
        }
    )
    rows: list[dict[str, object]] = []
    for month in (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-04-01")):
        for index, customer_id in enumerate(customers["customer_id"]):
            rows.append(
                {
                    "customer_id": customer_id,
                    "month": month,
                    "active_mrr": 100.0 + index,
                    "realized_price_index": 0.95 - index / 1000,
                    "avg_discount_pct": 0.05 + index / 500,
                    "discount_dependency_flag": int(index % 3 == 0),
                    "renewal_risk_proxy": index / 100,
                }
            )
    return pd.DataFrame(rows), customers


def _known_ledger() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "experiment_id": ["exp"] * 4,
            "assignment_id": ["a1", "a2", "a3", "a4"],
            "customer_id": ["t1", "t2", "c1", "c2"],
            "assignment_month": [pd.Timestamp("2025-01-01")] * 4,
            "assignment_group": ["treatment", "treatment", "control", "control"],
            "intervention_type": ["outreach", "outreach", "none", "none"],
            "account_manager_id": ["AM1"] * 4,
            "segment": ["SMB"] * 4,
            "risk_band": ["B1"] * 4,
            "pre_risk_score": [60.0, 62.0, 59.0, 61.0],
            "baseline_mrr": [100.0] * 4,
            "intervention_cost": [10.0, 10.0, 0.0, 0.0],
            "assignment_probability": [0.5] * 4,
        }
    )


class TestInterventionAssignment(unittest.TestCase):
    def test_assignment_is_deterministic_balanced_and_ignores_forward_values(self) -> None:
        account_monthly, customers = _account_history()
        config = AssignmentConfig(followup_months=3, eligible_risk_quantile=0.5, seed=7)
        first = build_randomized_assignment(account_monthly, customers, config)

        changed_future = account_monthly.copy()
        changed_future.loc[changed_future["month"].eq("2025-04-01"), "renewal_risk_proxy"] = 0.0
        second = build_randomized_assignment(changed_future, customers, config)
        pd.testing.assert_frame_equal(first, second)

        counts = first.groupby(["segment", "risk_band", "assignment_group"], observed=True).size().unstack(fill_value=0)
        self.assertTrue((counts["treatment"] - counts["control"]).abs().le(1).all())
        self.assertEqual(first["assignment_id"].nunique(), len(first))
        self.assertTrue(first["assignment_probability"].eq(0.5).all())

    def test_invalid_control_cost_is_rejected(self) -> None:
        ledger = _known_ledger()
        ledger.loc[ledger["assignment_group"].eq("control"), "intervention_cost"] = 1.0
        with self.assertRaisesRegex(ValueError, "Control assignments"):
            validate_assignment_ledger(ledger)


class TestInterventionEffectiveness(unittest.TestCase):
    def test_forward_outcomes_and_known_uplift(self) -> None:
        ledger = _known_ledger()
        account_monthly = pd.DataFrame(
            {
                "customer_id": ["t1", "t2", "c1", "c2"] * 2,
                "month": [pd.Timestamp("2025-01-01")] * 4 + [pd.Timestamp("2025-04-01")] * 4,
                "active_mrr": [100.0] * 4 + [100.0, 100.0, 100.0, 0.0],
            }
        )
        monthly_metrics = pd.DataFrame(
            {
                "customer_id": ["t1", "t2", "c1", "c2"] * 3,
                "month": np.repeat(pd.date_range("2025-02-01", periods=3, freq="MS"), 4),
                "churn_flag": [0] * 8 + [0, 0, 0, 1],
            }
        )
        outcomes = attach_forward_outcomes(ledger, account_monthly, monthly_metrics, followup_months=3)
        summary, balance, metadata = evaluate_intervention(
            outcomes,
            EffectivenessConfig(bootstrap_samples=100, seed=3),
        )

        overall = summary.loc[summary["scope"].eq("All")].iloc[0]
        self.assertAlmostEqual(float(overall["logo_retention_uplift"]), 0.5)
        self.assertAlmostEqual(float(overall["gross_mrr_retention_uplift"]), 0.5)
        self.assertGreater(float(overall["commercial_roi"]), 0)
        self.assertEqual(overall["recommendation"], "continue_test")
        self.assertEqual(metadata["design"], "blocked_randomized_intent_to_treat")
        self.assertTrue(balance["absolute_smd"].ge(0).all())

    def test_baseline_mismatch_is_rejected(self) -> None:
        ledger = _known_ledger()
        account_monthly = pd.DataFrame(
            {
                "customer_id": ledger["customer_id"],
                "month": pd.Timestamp("2025-01-01"),
                "active_mrr": [99.0, 100.0, 100.0, 100.0],
            }
        )
        ending = account_monthly.assign(month=pd.Timestamp("2025-04-01"), active_mrr=100.0)
        monthly_metrics = pd.DataFrame(
            {
                "customer_id": ledger["customer_id"],
                "month": pd.Timestamp("2025-02-01"),
                "churn_flag": 0,
            }
        )
        with self.assertRaisesRegex(ValueError, "baseline_mrr"):
            attach_forward_outcomes(
                ledger,
                pd.concat([account_monthly, ending], ignore_index=True),
                monthly_metrics,
                followup_months=3,
            )


if __name__ == "__main__":
    unittest.main()

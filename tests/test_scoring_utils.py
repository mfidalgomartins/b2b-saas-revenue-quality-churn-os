"""Unit tests for src/scoring/scoring_utils.py.

These tests pin the canonical scoring primitives so that any drift in
thresholds, weights, or component formulas is caught before propagating to
production scoring or the calibration backtest.
"""

from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.scoring.scoring_utils import (  # noqa: E402
    CHURN_WEIGHTS,
    DISCOUNT_DEPENDENCY_WEIGHTS,
    EXPANSION_QUALITY_WEIGHTS,
    GOVERNANCE_WEIGHTS,
    REVENUE_QUALITY_WEIGHTS,
    argmax_driver,
    clip01,
    component_contributions,
    compute_churn_components,
    compute_discount_dependency_components,
    compute_expansion_quality_components,
    compute_revenue_quality_components,
    quality_to_risk_tier,
    risk_tier,
    rolling_trailing_slope,
    score_from_components,
)


class TestWeights(unittest.TestCase):
    """Every weight set must sum to 1.0; otherwise the 0-100 score is uncalibrated."""

    def test_all_weight_sets_sum_to_one(self) -> None:
        for name, weights in [
            ("churn", CHURN_WEIGHTS),
            ("revenue_quality", REVENUE_QUALITY_WEIGHTS),
            ("discount_dependency", DISCOUNT_DEPENDENCY_WEIGHTS),
            ("expansion_quality", EXPANSION_QUALITY_WEIGHTS),
            ("governance", GOVERNANCE_WEIGHTS),
        ]:
            with self.subTest(name=name):
                self.assertTrue(math.isclose(sum(weights.values()), 1.0, abs_tol=1e-9))


class TestRiskTier(unittest.TestCase):
    def test_thresholds_are_inclusive_lower_exclusive_upper(self) -> None:
        cases = [
            (-10.0, "Low"),
            (0.0, "Low"),
            (29.999, "Low"),
            (30.0, "Moderate"),
            (54.999, "Moderate"),
            (55.0, "High"),
            (74.999, "High"),
            (75.0, "Critical"),
            (100.0, "Critical"),
        ]
        for score, expected in cases:
            with self.subTest(score=score):
                self.assertEqual(risk_tier(score), expected)

    def test_quality_inversion(self) -> None:
        # quality 80 => risk 20 => Low; quality 20 => risk 80 => Critical
        self.assertEqual(quality_to_risk_tier(80.0), "Low")
        self.assertEqual(quality_to_risk_tier(20.0), "Critical")


class TestClip01(unittest.TestCase):
    def test_clipping_bounds(self) -> None:
        result = clip01(np.array([-1.0, 0.0, 0.5, 1.0, 2.5]))
        np.testing.assert_array_equal(result, np.array([0.0, 0.0, 0.5, 1.0, 1.0]))


class TestScoreComposition(unittest.TestCase):
    def test_score_at_boundary(self) -> None:
        # All components zero => score zero
        comps = {"a": pd.Series([0.0, 0.0]), "b": pd.Series([0.0, 0.0])}
        weights = {"a": 0.6, "b": 0.4}
        result = score_from_components(comps, weights)
        np.testing.assert_array_almost_equal(result.to_numpy(), [0.0, 0.0])

    def test_score_at_max(self) -> None:
        # All components one => score 100
        comps = {"a": pd.Series([1.0]), "b": pd.Series([1.0])}
        weights = {"a": 0.7, "b": 0.3}
        self.assertAlmostEqual(float(score_from_components(comps, weights).iloc[0]), 100.0, places=2)

    def test_weighted_combination(self) -> None:
        # 0.4*0.5 + 0.6*0.25 = 0.35 => score = 35
        comps = {"a": pd.Series([0.5]), "b": pd.Series([0.25])}
        weights = {"a": 0.4, "b": 0.6}
        self.assertAlmostEqual(float(score_from_components(comps, weights).iloc[0]), 35.0, places=2)

    def test_empty_components_raises(self) -> None:
        with self.assertRaises(ValueError):
            score_from_components({}, {})

    def test_component_contributions_pointwise(self) -> None:
        comps = {"a": pd.Series([0.5, 1.0]), "b": pd.Series([0.0, 0.5])}
        weights = {"a": 0.2, "b": 0.8}
        contrib = component_contributions(comps, weights)
        np.testing.assert_array_almost_equal(contrib["a"].to_numpy(), [0.10, 0.20])
        np.testing.assert_array_almost_equal(contrib["b"].to_numpy(), [0.00, 0.40])


class TestComputeChurnComponents(unittest.TestCase):
    """The component formulas must remain stable: a healthy account scores low, a stressed one scores high."""

    def _row(self, **overrides: float) -> pd.DataFrame:
        defaults = {
            "trailing_3m_usage_avg": 75.0,
            "trailing_3m_usage_trend": 0.5,
            "trailing_3m_nps_avg": 40.0,
            "trailing_3m_support_ticket_avg": 2.0,
            "trailing_3m_payment_delay_avg": 4.0,
            "trailing_3m_discount_avg": 0.05,
            "contraction_frequency": 0.0,
            "seat_growth_rate": 0.1,
            "heavy_discount_frequency_12m": 0.0,
            "renewal_due_flag": 0,
            "renewal_risk_proxy": 0.1,
            "churn_history_flag": 0,
            "tenure_months": 18,
        }
        defaults.update(overrides)
        return pd.DataFrame([defaults])

    def test_healthy_account_scores_near_zero(self) -> None:
        components = compute_churn_components(self._row())
        score = float(score_from_components(components, CHURN_WEIGHTS).iloc[0])
        self.assertLess(score, 10.0, f"Healthy account scored {score:.2f}, expected <10")

    def test_stressed_account_scores_high(self) -> None:
        stressed = self._row(
            trailing_3m_usage_avg=20.0,
            trailing_3m_usage_trend=-5.0,
            trailing_3m_nps_avg=-40.0,
            trailing_3m_support_ticket_avg=15.0,
            trailing_3m_payment_delay_avg=40.0,
            trailing_3m_discount_avg=0.40,
            contraction_frequency=0.40,
            seat_growth_rate=-0.30,
            heavy_discount_frequency_12m=0.70,
            renewal_due_flag=1,
            renewal_risk_proxy=0.80,
            churn_history_flag=1,
            tenure_months=2,
        )
        components = compute_churn_components(stressed)
        score = float(score_from_components(components, CHURN_WEIGHTS).iloc[0])
        self.assertGreater(score, 75.0, f"Stressed account scored {score:.2f}, expected >75")

    def test_components_in_unit_interval(self) -> None:
        # 50 random-ish rows should never produce components outside [0, 1]
        rng = np.random.default_rng(0)
        df = pd.DataFrame(
            {
                "trailing_3m_usage_avg": rng.uniform(0, 100, 50),
                "trailing_3m_usage_trend": rng.uniform(-10, 10, 50),
                "trailing_3m_nps_avg": rng.uniform(-100, 100, 50),
                "trailing_3m_support_ticket_avg": rng.uniform(0, 30, 50),
                "trailing_3m_payment_delay_avg": rng.uniform(0, 90, 50),
                "trailing_3m_discount_avg": rng.uniform(0, 0.55, 50),
                "contraction_frequency": rng.uniform(0, 1, 50),
                "seat_growth_rate": rng.uniform(-0.5, 0.5, 50),
                "heavy_discount_frequency_12m": rng.uniform(0, 1, 50),
                "renewal_due_flag": rng.integers(0, 2, 50),
                "renewal_risk_proxy": rng.uniform(0, 1, 50),
                "churn_history_flag": rng.integers(0, 2, 50),
                "tenure_months": rng.integers(0, 120, 50),
            }
        )
        for name, series in compute_churn_components(df).items():
            with self.subTest(component=name):
                self.assertTrue((series >= 0).all(), f"{name} has negative values")
                self.assertTrue((series <= 1).all(), f"{name} exceeds 1")


class TestArgmaxDriver(unittest.TestCase):
    """argmax_driver names the dominant contribution; ties resolve to the first key."""

    def test_picks_largest_contribution_and_maps_label(self) -> None:
        row = pd.Series({"churn_c": 0.1, "discount_c": 0.7, "renewal_c": 0.2})
        mapping = {"discount_c": "Discount pressure"}
        keys = ["churn_c", "discount_c", "renewal_c"]
        self.assertEqual(argmax_driver(row, mapping, keys), "Discount pressure")

    def test_falls_back_to_key_when_unmapped(self) -> None:
        row = pd.Series({"a": 0.9, "b": 0.1})
        self.assertEqual(argmax_driver(row, {}, ["a", "b"]), "a")

    def test_tie_resolves_to_first_key(self) -> None:
        row = pd.Series({"a": 0.5, "b": 0.5})
        self.assertEqual(argmax_driver(row, {}, ["a", "b"]), "a")


class TestScoreComponentFamilies(unittest.TestCase):
    """The three centralised component families must align with their weight sets,
    stay in [0, 1] for in-range inputs, and compose to a [0, 100] score."""

    FAMILIES = (
        ("revenue_quality", compute_revenue_quality_components, REVENUE_QUALITY_WEIGHTS),
        ("discount_dependency", compute_discount_dependency_components, DISCOUNT_DEPENDENCY_WEIGHTS),
        ("expansion_quality", compute_expansion_quality_components, EXPANSION_QUALITY_WEIGHTS),
    )

    def _features(self) -> pd.DataFrame:
        rng = np.random.default_rng(11)
        n = 60
        return pd.DataFrame(
            {
                # revenue-quality inputs
                "realized_price_index": rng.uniform(0.5, 1.1, n),
                "trailing_3m_discount_avg": rng.uniform(0.0, 0.5, n),
                "contraction_frequency": rng.uniform(0.0, 1.0, n),
                "expansion_frequency": rng.uniform(0.0, 1.0, n),
                "trailing_3m_usage_avg": rng.uniform(0.0, 100.0, n),
                "trailing_3m_nps_avg": rng.uniform(-100.0, 100.0, n),
                "trailing_3m_payment_delay_avg": rng.uniform(0.0, 60.0, n),
                "renewal_risk_proxy": rng.uniform(0.0, 1.0, n),
                "churn_history_flag": rng.integers(0, 2, n).astype(float),
                "quality_flag_health_factor": rng.uniform(0.15, 1.0, n),
                # discount-dependency inputs
                "heavy_discount_frequency_12m": rng.uniform(0.0, 1.0, n),
                "discounted_expansion_share_12m": rng.uniform(0.0, 1.0, n),
                "discount_dependency_flag": rng.integers(0, 2, n).astype(float),
                "manager_discount_outlier_flag": rng.integers(0, 2, n).astype(float),
                # expansion-quality inputs
                "healthy_expansion_ratio_12m": rng.uniform(0.0, 1.0, n),
                "fragile_expansion_ratio_12m": rng.uniform(0.0, 1.0, n),
                "avg_expansion_discount_12m": rng.uniform(0.0, 0.5, n),
                "avg_expansion_payment_delay_12m": rng.uniform(0.0, 40.0, n),
                "post_expansion_contraction_rate_3m": rng.uniform(0.0, 1.0, n),
            }
        )

    def test_component_keys_match_weight_sets(self) -> None:
        features = self._features()
        for name, fn, weights in self.FAMILIES:
            with self.subTest(family=name):
                self.assertEqual(set(fn(features)), set(weights))

    def test_components_in_unit_interval(self) -> None:
        features = self._features()
        for name, fn, _ in self.FAMILIES:
            for key, series in fn(features).items():
                with self.subTest(family=name, component=key):
                    arr = np.asarray(series, dtype=float)
                    self.assertTrue((arr >= 0).all(), f"{name}.{key} below 0")
                    self.assertTrue((arr <= 1).all(), f"{name}.{key} above 1")

    def test_scores_compose_into_0_100(self) -> None:
        features = self._features()
        for name, fn, weights in self.FAMILIES:
            score = score_from_components(fn(features), weights)
            with self.subTest(family=name):
                self.assertTrue((score >= 0).all() and (score <= 100).all())

    def test_revenue_quality_directionality(self) -> None:
        # A pristine account should out-score a degraded one on revenue quality.
        good = pd.DataFrame(
            [
                {
                    "realized_price_index": 1.05,
                    "trailing_3m_discount_avg": 0.02,
                    "contraction_frequency": 0.0,
                    "expansion_frequency": 0.5,
                    "trailing_3m_usage_avg": 90.0,
                    "trailing_3m_nps_avg": 60.0,
                    "trailing_3m_payment_delay_avg": 1.0,
                    "renewal_risk_proxy": 0.05,
                    "churn_history_flag": 0.0,
                    "quality_flag_health_factor": 1.0,
                }
            ]
        )
        bad = pd.DataFrame(
            [
                {
                    "realized_price_index": 0.6,
                    "trailing_3m_discount_avg": 0.45,
                    "contraction_frequency": 0.6,
                    "expansion_frequency": 0.0,
                    "trailing_3m_usage_avg": 20.0,
                    "trailing_3m_nps_avg": -50.0,
                    "trailing_3m_payment_delay_avg": 45.0,
                    "renewal_risk_proxy": 0.9,
                    "churn_history_flag": 1.0,
                    "quality_flag_health_factor": 0.15,
                }
            ]
        )
        good_score = float(
            score_from_components(compute_revenue_quality_components(good), REVENUE_QUALITY_WEIGHTS).iloc[0]
        )
        bad_score = float(
            score_from_components(compute_revenue_quality_components(bad), REVENUE_QUALITY_WEIGHTS).iloc[0]
        )
        self.assertGreater(good_score, bad_score)
        self.assertGreater(good_score, 75.0)
        self.assertLess(bad_score, 25.0)


class TestRollingTrailingSlope(unittest.TestCase):
    """The vectorised slope must reproduce np.polyfit on every trailing window.

    This is the equivalence the backtest performance optimisation relies on: the
    closed-form replaces a per-window SVD solve, so it must match it.
    """

    @staticmethod
    def _polyfit_reference(values: np.ndarray, window: int) -> np.ndarray:
        # The original implementation: rolling polyfit, min_periods=1, NaNs dropped.
        def slope(arr: np.ndarray) -> float:
            v = arr[~np.isnan(arr)]
            if len(v) < 2:
                return 0.0
            x = np.arange(len(v))
            return float(np.polyfit(x, v, 1)[0])

        return pd.Series(values).rolling(window=window, min_periods=1).apply(slope, raw=True).to_numpy()

    def test_matches_polyfit_on_random_series(self) -> None:
        rng = np.random.default_rng(7)
        for trial in range(200):
            n = int(rng.integers(1, 20))
            values = rng.uniform(-50, 100, n)
            got = rolling_trailing_slope(values, window=3)
            want = self._polyfit_reference(values, window=3)
            with self.subTest(trial=trial, n=n):
                np.testing.assert_allclose(got, want, atol=1e-9, rtol=0)

    def test_known_small_windows(self) -> None:
        v = np.array([10.0, 14.0, 12.0, 20.0])
        got = rolling_trailing_slope(v, window=3)
        # pos0 -> 0; pos1 -> 14-10=4; pos2 -> (12-10)/2=1; pos3 -> (20-14)/2=3
        np.testing.assert_allclose(got, [0.0, 4.0, 1.0, 3.0])

    def test_window_two_matches_polyfit(self) -> None:
        rng = np.random.default_rng(3)
        values = rng.uniform(0, 10, 12)
        np.testing.assert_allclose(
            rolling_trailing_slope(values, window=2),
            self._polyfit_reference(values, window=2),
            atol=1e-9,
        )

    def test_single_point_and_empty(self) -> None:
        np.testing.assert_array_equal(rolling_trailing_slope(np.array([5.0])), [0.0])
        self.assertEqual(len(rolling_trailing_slope(np.array([]))), 0)

    def test_window_above_three_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            rolling_trailing_slope(np.array([1.0, 2.0, 3.0, 4.0]), window=4)


if __name__ == "__main__":
    unittest.main()

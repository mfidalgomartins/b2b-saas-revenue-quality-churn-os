"""Weight sensitivity analysis for the governance priority score.

Perturbs each `GOVERNANCE_WEIGHTS` entry by ±20% (one at a time), renormalises
the rest of the weights so they still sum to 1, recomputes the governance
priority score, and reports how many accounts flip tier (Low / Moderate /
High / Critical).

A model where small weight changes cascade into large tier flips is fragile.
A model that's stable under ±20% perturbation is one a CRO can defend.

Writes:
  reports/weight_sensitivity_report.json
  reports/weight_sensitivity_report.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.scoring.scoring_utils import (  # noqa: E402
    GOVERNANCE_WEIGHTS,
    clip01,
    risk_tier,
    score_from_components,
)

logger = logging.getLogger("weight-sensitivity")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--perturbation", type=float, default=0.20,
                        help="Fractional shock applied to each weight (default 0.20).")
    parser.add_argument("--json-output", type=str, default="reports/weight_sensitivity_report.json")
    parser.add_argument("--csv-output", type=str, default="reports/weight_sensitivity_report.csv")
    return parser.parse_args()


def rebuild_governance_components(scoring: pd.DataFrame) -> dict[str, pd.Series]:
    """Recompose the governance components from the persisted scoring table."""
    p99_mrr = float(scoring["current_mrr"].quantile(0.99))
    exposure = 0.70 * clip01(np.log1p(scoring["current_mrr"]) / np.log1p(max(p99_mrr, 1.0))) \
        + 0.30 * clip01(scoring["concentration_weight"] / 0.01)
    return {
        "churn_risk": scoring["churn_risk_score"] / 100.0,
        "revenue_quality_risk": (100.0 - scoring["revenue_quality_score"]) / 100.0,
        "discount_dependency": scoring["discount_dependency_score"] / 100.0,
        "expansion_fragility": (100.0 - scoring["expansion_quality_score"]) / 100.0,
        "exposure_concentration": exposure,
        "renewal_urgency": clip01(
            scoring["renewal_due_flag"] * scoring["renewal_risk_proxy"]
            + scoring["renewal_due_flag"] * 0.15
        ),
    }


def perturb(weights: dict[str, float], key: str, factor: float) -> dict[str, float]:
    """Shock one weight by `factor` and renormalise the rest to sum to 1."""
    new = dict(weights)
    new[key] = max(0.0, weights[key] * (1.0 + factor))
    total = sum(new.values())
    return {k: v / total for k, v in new.items()}


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    base_dir = Path(args.base_dir).resolve()
    scoring = pd.read_csv(base_dir / "data" / "processed" / "account_scoring_model_output.csv")
    amrq = pd.read_csv(
        base_dir / "data" / "processed" / "account_monthly_revenue_quality.csv",
        parse_dates=["month"],
    )
    latest = (
        amrq.sort_values("month")
        .groupby("customer_id", as_index=False)
        .tail(1)[["customer_id", "renewal_risk_proxy"]]
    )
    scoring = scoring.merge(latest, on="customer_id", how="left")
    scoring["renewal_risk_proxy"] = scoring["renewal_risk_proxy"].fillna(0.0)

    components = rebuild_governance_components(scoring)

    baseline_score = score_from_components(components, GOVERNANCE_WEIGHTS)
    baseline_tier = baseline_score.apply(risk_tier)

    rows = []
    for key in GOVERNANCE_WEIGHTS:
        for direction, factor in (("up", +args.perturbation), ("down", -args.perturbation)):
            new_weights = perturb(GOVERNANCE_WEIGHTS, key, factor)
            new_score = score_from_components(components, new_weights)
            new_tier = new_score.apply(risk_tier)
            flips = int((new_tier != baseline_tier).sum())
            rows.append({
                "weight": key,
                "direction": direction,
                "factor": factor,
                "new_weight": round(new_weights[key], 4),
                "tier_flips": flips,
                "tier_flip_pct": round(flips / len(baseline_tier), 6),
                "mean_score_delta": round(float((new_score - baseline_score).mean()), 4),
                "max_score_delta": round(float((new_score - baseline_score).abs().max()), 4),
            })

    df = pd.DataFrame(rows)
    df = df.sort_values("tier_flip_pct", ascending=False).reset_index(drop=True)

    csv_out = base_dir / args.csv_output
    json_out = base_dir / args.json_output
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_out, index=False)

    worst = df.iloc[0]
    summary = {
        "n_accounts": int(len(baseline_tier)),
        "perturbation": args.perturbation,
        "baseline_weights": dict(GOVERNANCE_WEIGHTS),
        "max_tier_flip_pct": float(worst["tier_flip_pct"]),
        "max_tier_flip_lever": f"{worst['weight']} ({worst['direction']})",
        "mean_tier_flip_pct": round(float(df["tier_flip_pct"].mean()), 6),
        "all_under_5pct": bool((df["tier_flip_pct"] < 0.05).all()),
    }
    json_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    logger.info("weight sensitivity complete (n=%d, max flip=%.2f%% on %s)",
                summary["n_accounts"],
                summary["max_tier_flip_pct"] * 100,
                summary["max_tier_flip_lever"])
    logger.info("  csv : %s", csv_out)
    logger.info("  json: %s", json_out)


if __name__ == "__main__":
    main()

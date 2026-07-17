"""Build an intervention assignment ledger, outcomes, uplift and ROI evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.interventions.assignment import AssignmentConfig, build_randomized_assignment, validate_assignment_ledger
from src.interventions.effectiveness import EffectivenessConfig, attach_forward_outcomes, evaluate_intervention
from src.io.logging_setup import get_logger

log = get_logger(__name__)


def _format_pct(value: float) -> str:
    return f"{value:.2%}"


def _build_memo(metadata: dict[str, Any], balance: pd.DataFrame) -> str:
    result = metadata["overall"]
    return f"""# Intervention effectiveness decision memo

## Decision

**{str(result["recommendation"]).replace("_", " ").title()}** for the success-plan outreach treatment. The estimated gross MRR retention uplift is {_format_pct(result["gross_mrr_retention_uplift"])} (95% CI {_format_pct(result["mrr_uplift_ci_lower"])} to {_format_pct(result["mrr_uplift_ci_upper"])}); annualized commercial ROI is {_format_pct(result["commercial_roi"])}.

## Experimental contract

- Estimand: intent-to-treat assignment effect among eligible active high-risk accounts.
- Unit: customer account; treatment is assignment to success-plan outreach; control is no action.
- Assignment: 50/50 randomization blocked by segment and pre-treatment risk band.
- Outcome window: {metadata["followup_months"]} months after assignment.
- Uncertainty: {metadata["confidence_interval"]} with {metadata["bootstrap_samples"]:,} samples.
- Commercial model: uplift × treated baseline MRR × {metadata["annualization_months"]} months × {metadata["gross_margin"]:.0%} gross margin, less observed intervention cost.

## Evidence

- Accounts: {result["n_total"]:,} ({result["n_treatment"]:,} treatment; {result["n_control"]:,} control).
- Logo retention uplift: {_format_pct(result["logo_retention_uplift"])} (95% CI {_format_pct(result["logo_uplift_ci_lower"])} to {_format_pct(result["logo_uplift_ci_upper"])}).
- Gross MRR retention uplift: {_format_pct(result["gross_mrr_retention_uplift"])} (95% CI {_format_pct(result["mrr_uplift_ci_lower"])} to {_format_pct(result["mrr_uplift_ci_upper"])}).
- Estimated incremental retained MRR: ${result["incremental_retained_mrr"]:,.0f}.
- Intervention cost: ${result["intervention_cost"]:,.0f}; annualized incremental gross profit: ${result["annualized_incremental_gross_profit"]:,.0f}.
- Largest absolute standardized mean difference: {balance["absolute_smd"].max():.3f} ({"within" if balance["absolute_smd"].max() <= 0.10 else "outside"} the 0.10 balance threshold).

## Interpretation boundary

This repository's experiment is assigned over synthetic operating data and demonstrates the measurement system, not external evidence that the treatment works in a real SaaS portfolio. Production use requires prospectively logged assignment, execution and cost data. The ITT estimate must remain primary even when some assigned accounts are not contacted; per-protocol cuts are diagnostic only.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", default=".")
    parser.add_argument(
        "--ledger-path", help="Use an existing assignment ledger instead of generating the synthetic trial."
    )
    parser.add_argument("--followup-months", type=int, default=3)
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--gross-margin", type=float, default=0.80)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    processed_dir = base_dir / "data/processed"
    reports_dir = base_dir / "reports"
    if not args.ledger_path and (base_dir / "data/raw/ingestion_manifest.json").exists():
        raise ValueError(
            "A governed real-data run requires --ledger-path; retrospective random assignment is not valid."
        )
    account_monthly = pd.read_csv(processed_dir / "account_monthly_revenue_quality.csv", parse_dates=["month"])
    monthly_metrics = pd.read_csv(base_dir / "data/raw/monthly_account_metrics.csv", parse_dates=["month"])

    if args.ledger_path:
        ledger = validate_assignment_ledger(pd.read_csv(Path(args.ledger_path), parse_dates=["assignment_month"]))
    else:
        customers = pd.read_csv(base_dir / "data/raw/customers.csv")
        ledger = build_randomized_assignment(
            account_monthly,
            customers,
            AssignmentConfig(followup_months=args.followup_months, seed=args.seed),
        )

    config = EffectivenessConfig(
        followup_months=args.followup_months,
        bootstrap_samples=args.bootstrap_samples,
        gross_margin=args.gross_margin,
        seed=args.seed,
    )
    outcomes = attach_forward_outcomes(ledger, account_monthly, monthly_metrics, args.followup_months)
    summary, balance, metadata = evaluate_intervention(outcomes, config)

    processed_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ledger.to_csv(processed_dir / "intervention_assignment_log.csv", index=False)
    outcomes.to_csv(processed_dir / "intervention_outcomes.csv", index=False)
    summary.to_csv(processed_dir / "intervention_effectiveness_by_segment.csv", index=False)
    balance.to_csv(processed_dir / "intervention_covariate_balance.csv", index=False)
    (reports_dir / "intervention_effectiveness_summary.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (reports_dir / "intervention_effectiveness_memo.md").write_text(_build_memo(metadata, balance), encoding="utf-8")
    log.info("Intervention evidence built: %s assignments", f"{len(ledger):,}")
    log.info("Decision: %s", metadata["overall"]["recommendation"])


if __name__ == "__main__":
    main()

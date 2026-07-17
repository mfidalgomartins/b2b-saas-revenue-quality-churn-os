"""End-to-end orchestrator for the revenue-quality analytics pipeline.

Builds source data, analytical and decision artifacts, validates the complete
release, refreshes the dashboard with the governed readiness status, and
enforces the publication gate.
Each stage is invoked as a subprocess so module-level state cannot leak between
steps; timings are logged for monthly performance tracking.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

logger = logging.getLogger("pipeline")


def run_step(name: str, cmd: list[str], cwd: Path) -> float:
    start = time.time()
    logger.info("step start: %s", name)
    result = subprocess.run(cmd, cwd=str(cwd))
    elapsed = time.time() - start
    if result.returncode != 0:
        raise RuntimeError(f"step failed [{name}] exit={result.returncode}: {' '.join(cmd)}")
    logger.info("step done : %s (%.1fs)", name, elapsed)
    return elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-data-generation", action="store_true")
    parser.add_argument(
        "--intervention-ledger",
        help="Prospectively captured assignment ledger. Required when using a governed real-data snapshot.",
    )
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--skip-gate", action="store_true")
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


def resolve_intervention_ledger(
    base_dir: Path,
    skip_data_generation: bool,
    ledger_value: str | None,
) -> str | None:
    """Resolve the ledger and block retrospective assignment on governed real data."""
    if ledger_value:
        ledger_path = Path(ledger_value).expanduser().resolve()
        if not ledger_path.is_file():
            raise FileNotFoundError(f"Intervention ledger not found: {ledger_path}")
        return str(ledger_path)
    real_manifest = base_dir / "data" / "raw" / "ingestion_manifest.json"
    if skip_data_generation and real_manifest.exists():
        raise ValueError(
            "A governed real-data run requires --intervention-ledger; retrospective random assignment is not valid."
        )
    return None


def build_steps(args: argparse.Namespace, py: str) -> list[tuple[str, list[str]]]:
    steps: list[tuple[str, list[str]]] = []

    if not args.skip_data_generation:
        steps.append(
            (
                "generate",
                [
                    py,
                    "-m",
                    "src.data_generation.generate_synthetic_data",
                    "--output-dir",
                    "data/raw",
                    "--note-path",
                    "docs/core/synthetic_data.md",
                    "--seed",
                    str(args.seed),
                ],
            )
        )

    intervention_cmd = [
        py,
        "-m",
        "src.interventions.build_intervention_effectiveness",
        "--base-dir",
        ".",
        "--seed",
        str(args.seed),
    ]
    if args.intervention_ledger:
        intervention_cmd.extend(["--ledger-path", args.intervention_ledger])

    steps.extend(
        [
            ("profile", [py, "-m", "src.profiling.build_data_profile", "--base-dir", "."]),
            (
                "features",
                [
                    py,
                    "-m",
                    "src.features.build_analytical_layer",
                    "--raw-dir",
                    "data/raw",
                    "--processed-dir",
                    "data/processed",
                    "--feature-dictionary-path",
                    "docs/core/feature_dictionary.md",
                    "--notes-path",
                    "docs/core/analytical_layer_notes.md",
                ],
            ),
            ("scoring", [py, "-m", "src.scoring.build_scoring_system", "--base-dir", "."]),
            ("backtest", [py, "-m", "src.scoring.backtest_scoring_calibration", "--base-dir", "."]),
            ("sensitivity", [py, "-m", "src.scoring.run_weight_sensitivity", "--base-dir", "."]),
            ("interventions", intervention_cmd),
            ("analysis", [py, "-m", "src.analysis.build_main_business_analysis", "--base-dir", "."]),
            ("forecast", [py, "-m", "src.forecasting.build_forecasting_scenarios", "--base-dir", "."]),
            (
                "probabilistic_forecast",
                [
                    py,
                    "-m",
                    "src.forecasting.build_probabilistic_forecast",
                    "--base-dir",
                    ".",
                    "--seed",
                    str(args.seed),
                ],
            ),
            ("graphs", [py, "-m", "src.visualization.build_executive_graphs", "--base-dir", "."]),
            (
                "supplementary_graphs",
                [py, "-m", "src.visualization.build_supplementary_graphs", "--base-dir", "."],
            ),
            (
                "dashboard",
                [
                    py,
                    "-m",
                    "src.dashboard.build_executive_dashboard",
                    "--base-dir",
                    ".",
                    "--output",
                    "outputs/dashboard/revenue-quality-command-center.html",
                ],
            ),
        ]
    )

    if not args.skip_validation:
        steps.append(("validate", [py, "-m", "src.validation.run_full_project_validation", "--base-dir", "."]))
        steps.append(
            (
                "dashboard_refresh",
                [
                    py,
                    "-m",
                    "src.dashboard.build_executive_dashboard",
                    "--base-dir",
                    ".",
                    "--output",
                    "outputs/dashboard/revenue-quality-command-center.html",
                ],
            )
        )
        if not args.skip_gate:
            steps.append(
                (
                    "gate",
                    [
                        py,
                        "-m",
                        "src.validation.check_validation_gate",
                        "--summary-path",
                        "reports/formal_validation_summary.json",
                        "--max-warn",
                        "0",
                        "--max-fail",
                        "0",
                        "--max-high-severity",
                        "0",
                        "--max-critical-severity",
                        "0",
                        "--min-readiness-tier",
                        "technically valid",
                    ],
                )
            )
    steps.append(("report", [py, "scripts/build_pdf_report.py"]))
    return steps


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    base_dir = Path(args.base_dir).resolve()
    args.intervention_ledger = resolve_intervention_ledger(
        base_dir,
        args.skip_data_generation,
        args.intervention_ledger,
    )
    steps = build_steps(args, py=sys.executable)

    timings: list[tuple[str, float]] = []
    for name, cmd in steps:
        timings.append((name, run_step(name, cmd, base_dir)))

    total = sum(t for _, t in timings)
    logger.info("pipeline complete in %.1fs", total)
    for name, elapsed in timings:
        logger.info("  %-18s %6.1fs", name, elapsed)


if __name__ == "__main__":
    main()

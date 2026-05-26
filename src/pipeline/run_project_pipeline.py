"""End-to-end orchestrator for the revenue-quality analytics pipeline.

Runs nine stages (generate → profile → features → score → backtest → analyze
→ forecast → visualize → dashboard) and a two-step release gate (validate →
enforce).
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
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--skip-gate", action="store_true")
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


def build_steps(args: argparse.Namespace, py: str) -> list[tuple[str, list[str]]]:
    steps: list[tuple[str, list[str]]] = []

    if not args.skip_data_generation:
        steps.append((
            "generate",
            [py, "src/data_generation/generate_synthetic_data.py",
             "--output-dir", "data/raw",
             "--note-path", "docs/core/synthetic_data.md",
             "--seed", str(args.seed)],
        ))

    steps.extend([
        ("profile", [py, "src/profiling/build_data_profile.py", "--base-dir", "."]),
        ("features", [
            py, "src/features/build_analytical_layer.py",
            "--raw-dir", "data/raw",
            "--processed-dir", "data/processed",
            "--feature-dictionary-path", "docs/core/feature_dictionary.md",
            "--notes-path", "docs/core/analytical_layer_notes.md",
        ]),
        ("scoring", [py, "src/scoring/build_scoring_system.py", "--base-dir", "."]),
        ("backtest", [py, "src/scoring/backtest_scoring_calibration.py", "--base-dir", "."]),
        ("sensitivity", [py, "src/scoring/run_weight_sensitivity.py", "--base-dir", "."]),
        ("analysis", [py, "src/analysis/build_main_business_analysis.py", "--base-dir", "."]),
        ("forecast", [py, "src/forecasting/build_forecasting_scenarios.py", "--base-dir", "."]),
        ("visualization", [py, "src/visualization/build_leadership_charts.py", "--base-dir", "."]),
        ("dashboard", [
            py, "src/dashboard/build_executive_dashboard.py",
            "--base-dir", ".",
            "--output", "outputs/dashboard/revenue-quality-command-center.html",
        ]),
    ])

    if not args.skip_validation:
        steps.append(("validate", [py, "src/validation/run_full_project_validation.py", "--base-dir", "."]))
        steps.append(("dashboard_refresh", [
            py, "src/dashboard/build_executive_dashboard.py",
            "--base-dir", ".",
            "--output", "outputs/dashboard/revenue-quality-command-center.html",
        ]))
        if not args.skip_gate:
            steps.append(("gate", [
                py, "src/validation/check_validation_gate.py",
                "--summary-path", "reports/formal_validation_summary.json",
                "--max-warn", "0", "--max-fail", "0",
                "--max-high-severity", "0", "--max-critical-severity", "0",
                "--min-readiness-tier", "technically valid",
            ]))
    return steps


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    base_dir = Path(args.base_dir).resolve()
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

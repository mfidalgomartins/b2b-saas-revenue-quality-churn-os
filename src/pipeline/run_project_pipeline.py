from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import List


def run_step(cmd: List[str], cwd: Path) -> float:
    start = time.time()
    result = subprocess.run(cmd, cwd=str(cwd))
    elapsed = time.time() - start
    if result.returncode != 0:
        raise RuntimeError(f"Step failed ({result.returncode}): {' '.join(cmd)}")
    return elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full B2B SaaS analytics pipeline end-to-end.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-data-generation", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--skip-gate", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()

    py = sys.executable
    steps: List[tuple[str, List[str]]] = []

    if not args.skip_data_generation:
        steps.append(
            (
                "data_generation",
                [
                    py,
                    "src/data_generation/generate_synthetic_data.py",
                    "--output-dir",
                    "data/raw",
                    "--note-path",
                    "docs/core/synthetic_data.md",
                    "--seed",
                    str(args.seed),
                ],
            )
        )

    steps.extend(
        [
            ("profiling", [py, "src/profiling/build_data_profile.py", "--base-dir", "."]),
            (
                "features",
                [
                    py,
                    "src/features/build_analytical_layer.py",
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
            ("scoring", [py, "src/scoring/build_scoring_system.py", "--base-dir", "."]),
            ("analysis", [py, "src/analysis/build_main_business_analysis.py", "--base-dir", "."]),
            ("forecasting", [py, "src/forecasting/build_forecasting_scenarios.py", "--base-dir", "."]),
            ("visualization", [py, "src/visualization/build_leadership_charts.py", "--base-dir", "."]),
            (
                "dashboard",
                [
                    py,
                    "src/dashboard/build_executive_dashboard.py",
                    "--base-dir",
                    ".",
                    "--output",
                    "outputs/dashboard/executive_dashboard.html",
                ],
            ),
        ]
    )

    if not args.skip_validation:
        steps.append(("validation", [py, "src/validation/run_full_project_validation.py", "--base-dir", "."]))
        steps.append(
            (
                "dashboard_refresh",
                [
                    py,
                    "src/dashboard/build_executive_dashboard.py",
                    "--base-dir",
                    ".",
                    "--output",
                    "outputs/dashboard/executive_dashboard.html",
                ],
            )
        )
        if not args.skip_gate:
            steps.append(
                (
                    "validation_gate",
                    [
                        py,
                        "src/validation/check_validation_gate.py",
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

    timings: List[tuple[str, float]] = []
    for name, cmd in steps:
        print(f"[START] {name}")
        elapsed = run_step(cmd, base_dir)
        timings.append((name, elapsed))
        print(f"[DONE]  {name} ({elapsed:.1f}s)")

    print("\nPipeline complete.")
    for name, elapsed in timings:
        print(f"- {name}: {elapsed:.1f}s")


if __name__ == "__main__":
    main()

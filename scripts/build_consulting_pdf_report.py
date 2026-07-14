"""Build the consulting-grade Revenue Quality Operating System report."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


BASE = Path(__file__).resolve().parents[1]
EXISTING_REPORT = BASE / "outputs" / "reports" / "revenue_quality_os_analytical_report.pdf"
DEFAULT_OUTPUT = BASE / "outputs" / "reports" / "revenue_quality_os_consulting_report.pdf"
REQUIRED_INPUTS = (
    Path("scripts/build_pdf_report.py"),
    Path("assets/fonts/Inter-Regular.ttf"),
    Path("assets/fonts/Inter-SemiBold.ttf"),
    Path("assets/fonts/SourceSerif4-Regular.ttf"),
    Path("assets/fonts/SourceSerif4-Bold.ttf"),
    Path("reports/main_business_analysis_metrics.json"),
    Path("reports/intervention_effectiveness_summary.json"),
    Path("reports/probabilistic_forecast_validation.json"),
    Path("data/processed/account_monthly_revenue_quality.csv"),
)


def sha256(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_inputs(base_dir: Path) -> None:
    """Fail fast when a source document, dataset or font is unavailable."""
    for relative_path in REQUIRED_INPUTS:
        path = base_dir / relative_path
        if not path.is_file():
            raise FileNotFoundError(f"Required report input is missing: {path}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the report output path."""
    parser = argparse.ArgumentParser(description="Build the consulting-grade revenue quality PDF.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)

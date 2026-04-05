from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run monthly refresh and emit release metadata/checksums.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-data-generation", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--release-tag", type=str, default="")
    parser.add_argument("--manifest-path", type=str, default="reports/release_manifest.json")
    parser.add_argument("--checksums-path", type=str, default="reports/release_checksums.csv")
    return parser.parse_args()


def run_step(cmd: List[str], cwd: Path) -> None:
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")


def hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def csv_row_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def month_coverage(monthly_metrics_path: Path) -> Dict[str, str]:
    with monthly_metrics_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        months = [row["month"][:10] for row in reader if row.get("month")]
    if not months:
        return {"min_month": "", "max_month": ""}
    return {"min_month": min(months), "max_month": max(months)}


def collect_artifacts(base_dir: Path, exclude_paths: set[Path] | None = None) -> List[Path]:
    exclude_paths = exclude_paths or set()
    patterns = [
        "README.md",
        "methodology.md",
        "data_dictionary.md",
        "executive_summary.md",
        "LICENSE",
        "requirements.txt",
        "requirements-notebook.txt",
        "Makefile",
        "data/raw/*.csv",
        "data/processed/*.csv",
        "docs/*.md",
        "notebooks/*.md",
        "notebooks/*.ipynb",
        "reports/*.md",
        "reports/*.json",
        "reports/*.csv",
        "outputs/charts/*.png",
        "outputs/dashboard/*.html",
        "sql/*.md",
        "sql/staging/*.sql",
        "sql/marts/*.sql",
    ]
    files: List[Path] = []
    for pattern in patterns:
        files.extend(sorted(base_dir.glob(pattern)))
    normalized_excludes = {p.resolve() for p in exclude_paths}
    return sorted({p for p in files if p.is_file() and p.resolve() not in normalized_excludes})


def try_create_release_tag(base_dir: Path, release_tag: str) -> Dict[str, str]:
    if not release_tag:
        return {"status": "skipped", "message": "No release tag requested."}

    is_git = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=str(base_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if is_git.returncode != 0 or "true" not in is_git.stdout.lower():
        return {"status": "skipped", "message": "Directory is not a git repository."}

    tag_exists = subprocess.run(
        ["git", "tag", "--list", release_tag],
        cwd=str(base_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if tag_exists.returncode == 0 and release_tag in tag_exists.stdout.split():
        return {"status": "exists", "message": f"Tag already exists: {release_tag}"}

    tag_cmd = ["git", "tag", "-a", release_tag, "-m", f"Monthly analytics release {release_tag}"]
    created = subprocess.run(tag_cmd, cwd=str(base_dir), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if created.returncode != 0:
        return {"status": "failed", "message": created.stderr.strip() or "Failed to create tag."}
    return {"status": "created", "message": f"Created tag {release_tag}"}


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    py = sys.executable

    pipeline_cmd = [
        py,
        "src/pipeline/run_project_pipeline.py",
        "--base-dir",
        ".",
        "--seed",
        str(args.seed),
    ]
    if args.skip_data_generation:
        pipeline_cmd.append("--skip-data-generation")
    if args.skip_validation:
        pipeline_cmd.append("--skip-validation")
    run_step(pipeline_cmd, base_dir)

    checksums_path = (base_dir / args.checksums_path).resolve()
    manifest_path = (base_dir / args.manifest_path).resolve()

    artifacts = collect_artifacts(base_dir, exclude_paths={checksums_path, manifest_path})
    checksum_rows: List[Dict[str, str | int]] = []
    for path in artifacts:
        checksum_rows.append(
            {
                "path": str(path.relative_to(base_dir)),
                "bytes": path.stat().st_size,
                "sha256": hash_file(path),
            }
        )

    checksums_path.parent.mkdir(parents=True, exist_ok=True)
    with checksums_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["path", "bytes", "sha256"])
        writer.writeheader()
        writer.writerows(checksum_rows)

    raw_dir = base_dir / "data" / "raw"
    processed_dir = base_dir / "data" / "processed"
    row_counts = {
        "raw": {p.name: csv_row_count(p) for p in sorted(raw_dir.glob("*.csv"))},
        "processed": {p.name: csv_row_count(p) for p in sorted(processed_dir.glob("*.csv"))},
    }
    coverage = month_coverage(raw_dir / "monthly_account_metrics.csv")

    validation_summary_path = base_dir / "reports" / "formal_validation_summary.json"
    validation_summary = {}
    if validation_summary_path.exists():
        validation_summary = json.loads(validation_summary_path.read_text(encoding="utf-8"))

    tag_result = try_create_release_tag(base_dir, args.release_tag)

    manifest = {
        "release_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "seed": args.seed,
        "pipeline_options": {
            "skip_data_generation": args.skip_data_generation,
            "skip_validation": args.skip_validation,
        },
        "data_coverage": coverage,
        "row_counts": row_counts,
        "artifact_count": len(checksum_rows),
        "checksums_path": str(checksums_path.relative_to(base_dir)),
        "validation_summary_path": str(validation_summary_path.relative_to(base_dir)) if validation_summary_path.exists() else "",
        "validation_summary": validation_summary.get("summary", {}),
        "release_tag_result": tag_result,
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Monthly release refresh complete.")
    print(f"Manifest: {manifest_path}")
    print(f"Checksums: {checksums_path}")
    print(f"Release tag status: {tag_result['status']} ({tag_result['message']})")


if __name__ == "__main__":
    main()

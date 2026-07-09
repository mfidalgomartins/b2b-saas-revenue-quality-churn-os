from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.io.logging_setup import get_logger  # noqa: E402

log = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the monthly revenue-quality refresh pipeline.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-data-generation", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--release-tag", type=str, default="")
    return parser.parse_args()


def run_step(cmd: list[str], cwd: Path) -> None:
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")


def collect_artifacts(base_dir: Path) -> list[Path]:
    patterns = [
        "README.md",
        "docs/core/*.md",
        "pyproject.toml",
        "data/raw/*.csv",
        "data/processed/*.csv",
        "notebooks/*.ipynb",
        "reports/*.md",
        "reports/*.json",
        "reports/*.csv",
        "outputs/graphs/*.png",
        "outputs/reports/*.pdf",
        "outputs/dashboard/*.html",
        "sql/*.md",
        "sql/staging/*.sql",
        "sql/marts/*.sql",
    ]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(base_dir.glob(pattern)))
    return sorted({p for p in files if p.is_file()})


def try_create_release_tag(base_dir: Path, release_tag: str) -> dict[str, str]:
    if not release_tag:
        return {"status": "skipped", "message": "No release tag requested."}

    is_git = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=str(base_dir),
        capture_output=True,
        text=True,
    )
    if is_git.returncode != 0 or "true" not in is_git.stdout.lower():
        return {"status": "skipped", "message": "Directory is not a git repository."}

    tag_exists = subprocess.run(
        ["git", "tag", "--list", release_tag],
        cwd=str(base_dir),
        capture_output=True,
        text=True,
    )
    if tag_exists.returncode == 0 and release_tag in tag_exists.stdout.split():
        return {"status": "exists", "message": f"Tag already exists: {release_tag}"}

    tag_cmd = ["git", "tag", "-a", release_tag, "-m", f"Monthly analytics release {release_tag}"]
    created = subprocess.run(tag_cmd, cwd=str(base_dir), capture_output=True, text=True)
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

    tag_result = try_create_release_tag(base_dir, args.release_tag)
    artifacts = collect_artifacts(base_dir)

    log.info("Monthly release refresh complete.")
    log.info("Artifacts refreshed: %s", len(artifacts))
    log.info("Release tag status: %s (%s)", tag_result["status"], tag_result["message"])


if __name__ == "__main__":
    main()

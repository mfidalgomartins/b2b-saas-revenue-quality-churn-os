"""CLI for governed ingestion of production CSV extracts."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from src.ingestion.contracts import load_ingestion_contract
from src.ingestion.csv_adapter import ingest_csv_source
from src.io.logging_setup import get_logger

log = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Path to the ingestion contract JSON.")
    parser.add_argument("--output-dir", help="Optional output-directory override.")
    parser.add_argument(
        "--evaluation-time",
        help="ISO-8601 SLA evaluation time. Defaults to the current UTC time.",
    )
    parser.add_argument(
        "--allow-partial-contract",
        action="store_true",
        help="Allow contracts that do not define the complete canonical pipeline schema.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    contract = load_ingestion_contract(Path(args.config))
    evaluation_time = datetime.fromisoformat(args.evaluation_time) if args.evaluation_time else None
    result = ingest_csv_source(
        contract,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        evaluation_time=evaluation_time,
        require_pipeline_contract=not args.allow_partial_contract,
    )
    log.info("Ingestion complete: %s", result.output_dir)
    for table_name, rows in sorted(result.table_rows.items()):
        log.info("%s: %s rows", table_name, f"{rows:,}")
    log.info("Manifest: %s", result.manifest_path)
    log.info("Publication allowed by contract: %s", result.publication_allowed)


if __name__ == "__main__":
    main()

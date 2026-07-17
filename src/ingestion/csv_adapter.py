"""CSV-directory adapter with data-quality SLAs and keyed pseudonymization."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.ingestion.contracts import IngestionContract, TableContract
from src.io.contracts import REQUIRED_RAW_SCHEMAS


class IngestionError(RuntimeError):
    """Raised when one or more source-contract checks fail."""

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        super().__init__("Ingestion blocked:\n- " + "\n- ".join(violations))


@dataclass(frozen=True)
class IngestionResult:
    output_dir: Path
    manifest_path: Path
    table_rows: dict[str, int]
    publication_allowed: bool


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pseudonymize(series: pd.Series, namespace: str, key: bytes, digest_length: int) -> pd.Series:
    def token(value: Any) -> Any:
        if pd.isna(value):
            return value
        message = f"{namespace}:{value}".encode()
        digest = hmac.new(key, message, hashlib.sha256).hexdigest()[:digest_length]
        return f"{namespace}_{digest}"

    return series.map(token)


def _load_key(contract: IngestionContract) -> bytes | None:
    if not contract.anonymization.enabled:
        return None
    key_env = contract.anonymization.key_env
    if key_env is None:
        raise IngestionError(["Anonymization is enabled but no key environment variable is configured"])
    raw = os.environ.get(key_env)
    if raw is None:
        raise IngestionError([f"Required anonymization key environment variable is not set: {key_env}"])
    key = raw.encode("utf-8")
    if len(key) < 16:
        raise IngestionError([f"Anonymization key in {key_env} must contain at least 16 UTF-8 bytes"])
    return key


def _transform_table(
    source_path: Path,
    table_name: str,
    table_contract: TableContract,
    key: bytes | None,
    digest_length: int,
) -> tuple[pd.DataFrame, list[str]]:
    violations: list[str] = []
    try:
        frame = pd.read_csv(source_path)
    except FileNotFoundError:
        return pd.DataFrame(), [f"{table_name}: source file not found: {source_path.name}"]
    except Exception as exc:
        return pd.DataFrame(), [f"{table_name}: source file cannot be read: {exc}"]

    missing_rename_sources = sorted(set(table_contract.rename_columns) - set(frame.columns))
    if missing_rename_sources:
        violations.append(f"{table_name}: rename source columns missing: {missing_rename_sources}")
    frame = frame.rename(columns=table_contract.rename_columns)
    if frame.columns.duplicated().any():
        violations.append(f"{table_name}: renaming produced duplicate canonical column names")

    missing_drop_columns = sorted(set(table_contract.drop_columns) - set(frame.columns))
    if missing_drop_columns:
        violations.append(f"{table_name}: configured PII drop columns missing: {missing_drop_columns}")
    frame = frame.drop(columns=list(table_contract.drop_columns), errors="ignore")

    missing_required = sorted(table_contract.required_columns - set(frame.columns))
    if missing_required:
        violations.append(f"{table_name}: required canonical columns missing: {missing_required}")
        return frame, violations

    for column in table_contract.parse_dates:
        try:
            frame[column] = pd.to_datetime(frame[column], errors="raise")
        except Exception as exc:
            violations.append(f"{table_name}.{column}: invalid date values: {exc}")

    if table_contract.anonymize_columns:
        if key is None:
            violations.append(f"{table_name}: anonymize_columns configured while anonymization is disabled")
        else:
            for column, namespace in table_contract.anonymize_columns.items():
                frame[column] = _pseudonymize(frame[column], namespace, key, digest_length=digest_length)

    row_count = len(frame)
    if row_count < table_contract.min_rows:
        violations.append(f"{table_name}: rows={row_count} below min_rows={table_contract.min_rows}")
    if table_contract.max_rows is not None and row_count > table_contract.max_rows:
        violations.append(f"{table_name}: rows={row_count} above max_rows={table_contract.max_rows}")

    null_primary_key = int(frame[list(table_contract.primary_key)].isna().any(axis=1).sum())
    duplicate_primary_key = int(frame.duplicated(list(table_contract.primary_key)).sum())
    if null_primary_key:
        violations.append(f"{table_name}: {null_primary_key} rows have null primary-key values")
    if duplicate_primary_key:
        violations.append(f"{table_name}: {duplicate_primary_key} duplicate primary-key rows")

    for column, threshold in table_contract.max_null_rate.items():
        actual = float(frame[column].isna().mean())
        if actual > threshold:
            violations.append(f"{table_name}.{column}: null rate {actual:.2%} exceeds SLA {threshold:.2%}")

    return frame, violations


def _validate_foreign_keys(contract: IngestionContract, tables: dict[str, pd.DataFrame]) -> list[str]:
    violations: list[str] = []
    for foreign_key in contract.foreign_keys:
        source = tables[foreign_key.table]
        reference = tables[foreign_key.references_table]
        source_keys = source[list(foreign_key.columns)].dropna().drop_duplicates()
        reference_keys = (
            reference[list(foreign_key.references_columns)]
            .dropna()
            .drop_duplicates()
            .rename(columns=dict(zip(foreign_key.references_columns, foreign_key.columns, strict=True)))
        )
        missing = source_keys.merge(reference_keys, on=list(foreign_key.columns), how="left", indicator=True)
        missing_count = int(missing["_merge"].eq("left_only").sum())
        if missing_count:
            violations.append(
                f"{foreign_key.table}{foreign_key.columns}: {missing_count} keys absent from "
                f"{foreign_key.references_table}{foreign_key.references_columns}"
            )
    return violations


def _validate_pipeline_schema(contract: IngestionContract) -> list[str]:
    violations: list[str] = []
    missing_tables = sorted(set(REQUIRED_RAW_SCHEMAS) - set(contract.tables))
    if missing_tables:
        violations.append(f"Pipeline contract is missing canonical tables: {missing_tables}")
    for table_name, required in REQUIRED_RAW_SCHEMAS.items():
        if table_name not in contract.tables:
            continue
        missing = sorted(required - contract.tables[table_name].required_columns)
        if missing:
            violations.append(f"Pipeline table '{table_name}' omits canonical fields: {missing}")
    return violations


def ingest_csv_source(
    contract: IngestionContract,
    *,
    output_dir: Path | None = None,
    evaluation_time: datetime | None = None,
    require_pipeline_contract: bool = True,
) -> IngestionResult:
    """Validate, pseudonymize and publish a canonical CSV snapshot.

    Every table and foreign key is validated in memory before any output file is
    written. A failed run therefore leaves the previous published snapshot intact.
    """
    evaluated_at = evaluation_time or datetime.now(UTC)
    if evaluated_at.tzinfo is None:
        evaluated_at = evaluated_at.replace(tzinfo=UTC)
    key = _load_key(contract)
    violations = _validate_pipeline_schema(contract) if require_pipeline_contract else []

    tables: dict[str, pd.DataFrame] = {}
    source_metadata: dict[str, dict[str, Any]] = {}
    for table_name, table_contract in contract.tables.items():
        source_path = (contract.source_dir / table_contract.file).resolve()
        if not source_path.is_relative_to(contract.source_dir):
            violations.append(f"{table_name}: source file resolves outside the configured source directory")
            tables[table_name] = pd.DataFrame()
            continue
        frame, table_violations = _transform_table(
            source_path,
            table_name,
            table_contract,
            key,
            contract.anonymization.digest_length,
        )
        violations.extend(table_violations)
        tables[table_name] = frame

        if source_path.exists():
            modified_at = datetime.fromtimestamp(source_path.stat().st_mtime, tz=UTC)
            file_age_hours = max(0.0, (evaluated_at - modified_at).total_seconds() / 3600)
            if table_contract.max_file_age_hours is not None and file_age_hours > table_contract.max_file_age_hours:
                violations.append(
                    f"{table_name}: source file age {file_age_hours:.1f}h exceeds SLA "
                    f"{table_contract.max_file_age_hours:.1f}h"
                )

            max_record_at: datetime | None = None
            record_age_days: float | None = None
            if table_contract.freshness_column is not None and table_contract.freshness_column in frame:
                latest = frame[table_contract.freshness_column].dropna().max()
                if pd.notna(latest):
                    latest_timestamp = pd.Timestamp(latest)
                    if latest_timestamp.tzinfo is None:
                        latest_timestamp = latest_timestamp.tz_localize(UTC)
                    max_record_at = latest_timestamp.to_pydatetime()
                    record_age_days = max(0.0, (evaluated_at - max_record_at).total_seconds() / 86400)
                    max_age = table_contract.max_record_age_days
                    if max_age is not None and record_age_days > max_age:
                        violations.append(
                            f"{table_name}: latest record age {record_age_days:.1f}d exceeds SLA {max_age}d"
                        )

            source_metadata[table_name] = {
                "source_file": table_contract.file,
                "source_sha256": _sha256(source_path),
                "source_modified_at": modified_at.isoformat(),
                "source_age_hours": round(file_age_hours, 3),
                "latest_record_at": max_record_at.isoformat() if max_record_at else None,
                "latest_record_age_days": round(record_age_days, 3) if record_age_days is not None else None,
            }

    if all(not frame.empty or contract.tables[name].min_rows == 0 for name, frame in tables.items()):
        violations.extend(_validate_foreign_keys(contract, tables))
    if violations:
        raise IngestionError(violations)

    destination = (output_dir or contract.output_dir).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    output_hashes: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="ingestion-", dir=destination.parent) as staging_dir_raw:
        staging_dir = Path(staging_dir_raw)
        for table_name, frame in tables.items():
            path = staging_dir / f"{table_name}.csv"
            frame.to_csv(path, index=False, encoding="utf-8", lineterminator="\n")
            output_hashes[table_name] = _sha256(path)

        manifest = {
            "manifest_version": 1,
            "source_name": contract.source_name,
            "evaluated_at": evaluated_at.isoformat(),
            "contract_sha256": _sha256(contract.contract_path),
            "publication_allowed": contract.publication_allowed,
            "anonymization_enabled": contract.anonymization.enabled,
            "status": "PASS",
            "tables": {
                table_name: {
                    **source_metadata[table_name],
                    "output_file": f"{table_name}.csv",
                    "output_sha256": output_hashes[table_name],
                    "rows": len(tables[table_name]),
                    "columns": len(tables[table_name].columns),
                    "primary_key": list(contract.tables[table_name].primary_key),
                }
                for table_name in sorted(tables)
            },
            "foreign_keys_checked": len(contract.foreign_keys),
        }
        staged_manifest = staging_dir / "ingestion_manifest.json"
        staged_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        destination.mkdir(parents=True, exist_ok=True)
        # Remove the previous commit marker before promotion. If publication is
        # interrupted, downstream governance sees no valid manifest rather than
        # trusting stale provenance for a partially replaced snapshot.
        (destination / "ingestion_manifest.json").unlink(missing_ok=True)
        for staged_path in sorted(staging_dir.glob("*.csv")):
            staged_path.replace(destination / staged_path.name)
        (destination / "synthetic_data_manifest.json").unlink(missing_ok=True)
        staged_manifest.replace(destination / staged_manifest.name)

    manifest_path = destination / "ingestion_manifest.json"
    return IngestionResult(
        output_dir=destination,
        manifest_path=manifest_path,
        table_rows={name: len(frame) for name, frame in tables.items()},
        publication_allowed=contract.publication_allowed,
    )

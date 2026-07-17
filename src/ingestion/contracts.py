"""Strict configuration contract for CSV-based production ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    """Raised when an ingestion contract is incomplete or internally inconsistent."""


@dataclass(frozen=True)
class AnonymizationConfig:
    enabled: bool
    key_env: str | None
    digest_length: int = 20


@dataclass(frozen=True)
class TableContract:
    file: str
    required_columns: frozenset[str]
    primary_key: tuple[str, ...]
    parse_dates: tuple[str, ...]
    rename_columns: dict[str, str]
    drop_columns: tuple[str, ...]
    anonymize_columns: dict[str, str]
    max_null_rate: dict[str, float]
    min_rows: int
    max_rows: int | None
    max_file_age_hours: float | None
    freshness_column: str | None
    max_record_age_days: int | None


@dataclass(frozen=True)
class ForeignKeyContract:
    table: str
    columns: tuple[str, ...]
    references_table: str
    references_columns: tuple[str, ...]


@dataclass(frozen=True)
class IngestionContract:
    version: int
    source_name: str
    source_dir: Path
    output_dir: Path
    publication_allowed: bool
    anonymization: AnonymizationConfig
    tables: dict[str, TableContract]
    foreign_keys: tuple[ForeignKeyContract, ...]
    contract_path: Path


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"'{field}' must be a JSON object")
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"'{field}' must be a non-empty string")
    return value.strip()


def _string_tuple(value: Any, field: str, *, allow_empty: bool = True) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ContractError(f"'{field}' must be a list of non-empty strings")
    if not allow_empty and not value:
        raise ContractError(f"'{field}' cannot be empty")
    if len(value) != len(set(value)):
        raise ContractError(f"'{field}' contains duplicate values")
    return tuple(value)


def _optional_positive_number(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ContractError(f"'{field}' must be null or a positive number")
    return float(value)


def _resolve_path(config_path: Path, value: Any, field: str) -> Path:
    path = Path(_string(value, field)).expanduser()
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def _validate_known_keys(payload: dict[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ContractError(f"Unknown keys in '{field}': {unknown}")


def _load_anonymization(payload: Any) -> AnonymizationConfig:
    config = _object(payload, "anonymization")
    _validate_known_keys(config, {"enabled", "key_env", "digest_length"}, "anonymization")
    enabled = config.get("enabled")
    if not isinstance(enabled, bool):
        raise ContractError("'anonymization.enabled' must be true or false")

    key_env_raw = config.get("key_env")
    key_env = None if key_env_raw is None else _string(key_env_raw, "anonymization.key_env")
    if enabled and key_env is None:
        raise ContractError("'anonymization.key_env' is required when anonymization is enabled")

    digest_length = config.get("digest_length", 20)
    if isinstance(digest_length, bool) or not isinstance(digest_length, int) or not 16 <= digest_length <= 64:
        raise ContractError("'anonymization.digest_length' must be an integer between 16 and 64")
    return AnonymizationConfig(enabled=enabled, key_env=key_env, digest_length=digest_length)


def _load_table(name: str, payload: Any) -> TableContract:
    config = _object(payload, f"tables.{name}")
    allowed = {
        "file",
        "required_columns",
        "primary_key",
        "parse_dates",
        "rename_columns",
        "drop_columns",
        "anonymize_columns",
        "max_null_rate",
        "min_rows",
        "max_rows",
        "max_file_age_hours",
        "freshness_column",
        "max_record_age_days",
    }
    _validate_known_keys(config, allowed, f"tables.{name}")

    required_columns = frozenset(
        _string_tuple(config.get("required_columns"), f"tables.{name}.required_columns", allow_empty=False)
    )
    primary_key = _string_tuple(config.get("primary_key"), f"tables.{name}.primary_key", allow_empty=False)
    parse_dates = _string_tuple(config.get("parse_dates", []), f"tables.{name}.parse_dates")
    drop_columns = _string_tuple(config.get("drop_columns", []), f"tables.{name}.drop_columns")

    rename_raw = _object(config.get("rename_columns", {}), f"tables.{name}.rename_columns")
    rename_columns = {
        _string(source, f"tables.{name}.rename_columns key"): _string(target, f"tables.{name}.rename_columns.{source}")
        for source, target in rename_raw.items()
    }
    if len(rename_columns.values()) != len(set(rename_columns.values())):
        raise ContractError(f"'tables.{name}.rename_columns' maps multiple fields to the same target")

    anonymize_raw = _object(config.get("anonymize_columns", {}), f"tables.{name}.anonymize_columns")
    anonymize_columns = {
        _string(column, f"tables.{name}.anonymize_columns key"): _string(
            namespace, f"tables.{name}.anonymize_columns.{column}"
        )
        for column, namespace in anonymize_raw.items()
    }

    null_raw = _object(config.get("max_null_rate", {}), f"tables.{name}.max_null_rate")
    max_null_rate: dict[str, float] = {}
    for column, threshold in null_raw.items():
        column_name = _string(column, f"tables.{name}.max_null_rate key")
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1:
            raise ContractError(f"'tables.{name}.max_null_rate.{column_name}' must be between 0 and 1")
        max_null_rate[column_name] = float(threshold)

    min_rows = config.get("min_rows", 1)
    if isinstance(min_rows, bool) or not isinstance(min_rows, int) or min_rows < 0:
        raise ContractError(f"'tables.{name}.min_rows' must be a non-negative integer")
    max_rows = config.get("max_rows")
    if max_rows is not None and (isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < min_rows):
        raise ContractError(f"'tables.{name}.max_rows' must be null or an integer >= min_rows")

    freshness_raw = config.get("freshness_column")
    freshness_column = None if freshness_raw is None else _string(freshness_raw, f"tables.{name}.freshness_column")
    max_record_age = config.get("max_record_age_days")
    if max_record_age is not None and (
        isinstance(max_record_age, bool) or not isinstance(max_record_age, int) or max_record_age < 0
    ):
        raise ContractError(f"'tables.{name}.max_record_age_days' must be null or a non-negative integer")
    if (freshness_column is None) != (max_record_age is None):
        raise ContractError(f"'tables.{name}.freshness_column' and 'max_record_age_days' must be configured together")

    configured_columns = (
        set(primary_key)
        | set(parse_dates)
        | set(max_null_rate)
        | set(anonymize_columns)
        | ({freshness_column} if freshness_column else set())
    )
    unknown_configured = sorted(configured_columns - required_columns)
    if unknown_configured:
        raise ContractError(
            f"Configured columns must be included in required_columns for table '{name}': {unknown_configured}"
        )
    dropped_required = sorted(set(drop_columns) & required_columns)
    if dropped_required:
        raise ContractError(f"Required canonical columns cannot be dropped in table '{name}': {dropped_required}")
    if set(primary_key) - required_columns:
        raise ContractError(f"Primary-key columns must be included in required_columns for table '{name}'")

    return TableContract(
        file=_string(config.get("file"), f"tables.{name}.file"),
        required_columns=required_columns,
        primary_key=primary_key,
        parse_dates=parse_dates,
        rename_columns=rename_columns,
        drop_columns=drop_columns,
        anonymize_columns=anonymize_columns,
        max_null_rate=max_null_rate,
        min_rows=min_rows,
        max_rows=max_rows,
        max_file_age_hours=_optional_positive_number(
            config.get("max_file_age_hours"), f"tables.{name}.max_file_age_hours"
        ),
        freshness_column=freshness_column,
        max_record_age_days=max_record_age,
    )


def _load_foreign_key(payload: Any, index: int, table_names: set[str]) -> ForeignKeyContract:
    field = f"foreign_keys[{index}]"
    config = _object(payload, field)
    _validate_known_keys(config, {"table", "columns", "references_table", "references_columns"}, field)
    table = _string(config.get("table"), f"{field}.table")
    references_table = _string(config.get("references_table"), f"{field}.references_table")
    columns = _string_tuple(config.get("columns"), f"{field}.columns", allow_empty=False)
    references_columns = _string_tuple(
        config.get("references_columns"), f"{field}.references_columns", allow_empty=False
    )
    if table not in table_names or references_table not in table_names:
        raise ContractError(f"'{field}' references a table not defined in the contract")
    if len(columns) != len(references_columns):
        raise ContractError(f"'{field}' must use the same number of source and reference columns")
    return ForeignKeyContract(table, columns, references_table, references_columns)


def load_ingestion_contract(path: Path) -> IngestionContract:
    """Load and validate an ingestion contract, resolving relative paths beside it."""
    contract_path = path.expanduser().resolve()
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ContractError(f"Ingestion contract not found: {contract_path}") from exc
    except json.JSONDecodeError as exc:
        raise ContractError(f"Invalid JSON in ingestion contract: {exc}") from exc

    root = _object(payload, "contract")
    _validate_known_keys(
        root,
        {
            "version",
            "source_name",
            "source_dir",
            "output_dir",
            "publication_allowed",
            "anonymization",
            "tables",
            "foreign_keys",
        },
        "contract",
    )
    version = root.get("version")
    if version != 1:
        raise ContractError("'version' must be 1")
    publication_allowed = root.get("publication_allowed")
    if not isinstance(publication_allowed, bool):
        raise ContractError("'publication_allowed' must be true or false")

    tables_raw = _object(root.get("tables"), "tables")
    if not tables_raw:
        raise ContractError("'tables' must define at least one table")
    tables = {_string(name, "tables key"): _load_table(str(name), value) for name, value in tables_raw.items()}
    foreign_keys_raw = root.get("foreign_keys", [])
    if not isinstance(foreign_keys_raw, list):
        raise ContractError("'foreign_keys' must be a list")
    foreign_keys = tuple(_load_foreign_key(value, index, set(tables)) for index, value in enumerate(foreign_keys_raw))

    for foreign_key in foreign_keys:
        if set(foreign_key.columns) - tables[foreign_key.table].required_columns:
            raise ContractError(f"Foreign-key source columns are not required in table '{foreign_key.table}'")
        if set(foreign_key.references_columns) - tables[foreign_key.references_table].required_columns:
            raise ContractError(
                f"Foreign-key reference columns are not required in table '{foreign_key.references_table}'"
            )

    return IngestionContract(
        version=1,
        source_name=_string(root.get("source_name"), "source_name"),
        source_dir=_resolve_path(contract_path, root.get("source_dir"), "source_dir"),
        output_dir=_resolve_path(contract_path, root.get("output_dir"), "output_dir"),
        publication_allowed=publication_allowed,
        anonymization=_load_anonymization(root.get("anonymization")),
        tables=tables,
        foreign_keys=foreign_keys,
        contract_path=contract_path,
    )

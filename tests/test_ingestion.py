"""Tests for governed CSV ingestion, SLAs and pseudonymization."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.ingestion.contracts import ContractError, load_ingestion_contract
from src.ingestion.csv_adapter import IngestionError, ingest_csv_source


class TestCsvIngestion(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source_dir = self.root / "source"
        self.source_dir.mkdir()
        self.output_dir = self.root / "canonical"
        self.contract_path = self.root / "ingestion.json"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _contract_payload(self) -> dict[str, object]:
        return {
            "version": 1,
            "source_name": "billing_export",
            "source_dir": "source",
            "output_dir": "canonical",
            "publication_allowed": False,
            "anonymization": {
                "enabled": True,
                "key_env": "TEST_INGESTION_KEY",
                "digest_length": 16,
            },
            "tables": {
                "customers": {
                    "file": "customers.csv",
                    "required_columns": ["customer_id", "signup_date", "segment"],
                    "primary_key": ["customer_id"],
                    "parse_dates": ["signup_date"],
                    "rename_columns": {"external_customer_id": "customer_id"},
                    "drop_columns": ["contact_email"],
                    "anonymize_columns": {"customer_id": "customer"},
                    "max_null_rate": {"segment": 0.0},
                    "min_rows": 2,
                    "freshness_column": "signup_date",
                    "max_record_age_days": 400,
                },
                "subscriptions": {
                    "file": "subscriptions.csv",
                    "required_columns": ["subscription_id", "customer_id", "realized_mrr"],
                    "primary_key": ["subscription_id"],
                    "parse_dates": [],
                    "rename_columns": {
                        "external_subscription_id": "subscription_id",
                        "external_customer_id": "customer_id",
                    },
                    "drop_columns": [],
                    "anonymize_columns": {
                        "subscription_id": "subscription",
                        "customer_id": "customer",
                    },
                    "max_null_rate": {"realized_mrr": 0.0},
                    "min_rows": 2,
                },
            },
            "foreign_keys": [
                {
                    "table": "subscriptions",
                    "columns": ["customer_id"],
                    "references_table": "customers",
                    "references_columns": ["customer_id"],
                }
            ],
        }

    def _write_contract(self, payload: dict[str, object] | None = None) -> None:
        document = payload or self._contract_payload()
        self.contract_path.write_text(json.dumps(document), encoding="utf-8")

    def _write_valid_sources(self) -> None:
        pd.DataFrame(
            {
                "external_customer_id": ["cust-1", "cust-2"],
                "signup_date": ["2025-12-01", "2025-12-15"],
                "segment": ["SMB", "Enterprise"],
                "contact_email": ["a@example.com", "b@example.com"],
            }
        ).to_csv(self.source_dir / "customers.csv", index=False)
        pd.DataFrame(
            {
                "external_subscription_id": ["sub-1", "sub-2"],
                "external_customer_id": ["cust-1", "cust-2"],
                "realized_mrr": [100.0, 250.0],
            }
        ).to_csv(self.source_dir / "subscriptions.csv", index=False)

    def test_valid_snapshot_is_pseudonymized_reconciled_and_reproducible(self) -> None:
        self._write_valid_sources()
        self._write_contract()
        contract = load_ingestion_contract(self.contract_path)
        evaluation_time = datetime(2026, 1, 1, tzinfo=UTC)

        with patch.dict(os.environ, {"TEST_INGESTION_KEY": "a-secure-test-key"}):
            first = ingest_csv_source(
                contract,
                evaluation_time=evaluation_time,
                require_pipeline_contract=False,
            )
            first_customers = (first.output_dir / "customers.csv").read_bytes()
            first_manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
            second = ingest_csv_source(
                contract,
                evaluation_time=evaluation_time,
                require_pipeline_contract=False,
            )

        customers = pd.read_csv(second.output_dir / "customers.csv")
        subscriptions = pd.read_csv(second.output_dir / "subscriptions.csv")
        self.assertNotIn("contact_email", customers.columns)
        self.assertTrue(customers["customer_id"].str.fullmatch(r"customer_[0-9a-f]{16}").all())
        self.assertEqual(set(customers["customer_id"]), set(subscriptions["customer_id"]))
        self.assertEqual((second.output_dir / "customers.csv").read_bytes(), first_customers)
        self.assertEqual(first_manifest["status"], "PASS")
        self.assertFalse(first_manifest["publication_allowed"])
        self.assertEqual(first_manifest["tables"]["subscriptions"]["rows"], 2)

    def test_missing_key_blocks_run_without_replacing_previous_snapshot(self) -> None:
        self._write_valid_sources()
        self._write_contract()
        self.output_dir.mkdir()
        sentinel = self.output_dir / "previous_snapshot.txt"
        sentinel.write_text("keep", encoding="utf-8")

        with patch.dict(os.environ, {}, clear=True), self.assertRaises(IngestionError) as ctx:
            ingest_csv_source(
                load_ingestion_contract(self.contract_path),
                require_pipeline_contract=False,
            )

        self.assertIn("TEST_INGESTION_KEY", str(ctx.exception))
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")
        self.assertFalse((self.output_dir / "ingestion_manifest.json").exists())

    def test_quality_violations_are_aggregated_before_publication(self) -> None:
        self._write_valid_sources()
        subscriptions_path = self.source_dir / "subscriptions.csv"
        subscriptions = pd.read_csv(subscriptions_path)
        subscriptions.loc[1, "external_subscription_id"] = "sub-1"
        subscriptions.loc[1, "external_customer_id"] = "unknown"
        subscriptions.loc[1, "realized_mrr"] = None
        subscriptions.to_csv(subscriptions_path, index=False)
        self._write_contract()

        with (
            patch.dict(os.environ, {"TEST_INGESTION_KEY": "a-secure-test-key"}),
            self.assertRaises(IngestionError) as ctx,
        ):
            ingest_csv_source(
                load_ingestion_contract(self.contract_path),
                evaluation_time=datetime(2026, 1, 1, tzinfo=UTC),
                require_pipeline_contract=False,
            )

        message = str(ctx.exception)
        self.assertIn("duplicate primary-key", message)
        self.assertIn("null rate", message)
        self.assertIn("keys absent", message)
        self.assertFalse(self.output_dir.exists())

    def test_unknown_contract_key_fails_fast(self) -> None:
        payload = self._contract_payload()
        payload["unexpected"] = True
        self._write_contract(payload)
        with self.assertRaisesRegex(ContractError, "Unknown keys"):
            load_ingestion_contract(self.contract_path)

    def test_source_path_cannot_escape_configured_directory(self) -> None:
        self._write_valid_sources()
        payload = self._contract_payload()
        tables = payload["tables"]
        assert isinstance(tables, dict)
        customers = tables["customers"]
        assert isinstance(customers, dict)
        customers["file"] = "../outside.csv"
        self._write_contract(payload)

        with (
            patch.dict(os.environ, {"TEST_INGESTION_KEY": "a-secure-test-key"}),
            self.assertRaisesRegex(IngestionError, "outside the configured source directory"),
        ):
            ingest_csv_source(
                load_ingestion_contract(self.contract_path),
                require_pipeline_contract=False,
            )


if __name__ == "__main__":
    unittest.main()

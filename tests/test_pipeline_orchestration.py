"""Tests for source-mode safety in the release orchestrator."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.pipeline.run_project_pipeline import resolve_intervention_ledger


class TestPipelineOrchestration(unittest.TestCase):
    def test_real_snapshot_requires_prospective_intervention_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            raw_dir = base_dir / "data" / "raw"
            raw_dir.mkdir(parents=True)
            (raw_dir / "ingestion_manifest.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "requires --intervention-ledger"):
                resolve_intervention_ledger(base_dir, skip_data_generation=True, ledger_value=None)

    def test_existing_ledger_is_resolved_for_real_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            ledger = base_dir / "assignment.csv"
            ledger.write_text("assignment_id\nASG-1\n", encoding="utf-8")

            resolved = resolve_intervention_ledger(base_dir, skip_data_generation=True, ledger_value=str(ledger))

            self.assertEqual(resolved, str(ledger.resolve()))

    def test_synthetic_snapshot_keeps_builtin_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            resolved = resolve_intervention_ledger(
                Path(temp_dir),
                skip_data_generation=True,
                ledger_value=None,
            )

            self.assertIsNone(resolved)


if __name__ == "__main__":
    unittest.main()

"""Unit tests for src/io/contracts.py and src/io/logging_setup.py.

These guard the load-boundary schema check and the shared logging setup — the
two pieces of plumbing every pipeline stage depends on.
"""

from __future__ import annotations

import logging
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.io.contracts import (  # noqa: E402
    REQUIRED_PROCESSED_SCHEMAS,
    REQUIRED_RAW_SCHEMAS,
    validate_schema,
)
from src.io.logging_setup import configure_logging, get_logger  # noqa: E402


class TestValidateSchema(unittest.TestCase):
    def test_passes_and_returns_same_frame_when_complete(self) -> None:
        required = frozenset({"a", "b"})
        df = pd.DataFrame({"a": [1], "b": [2], "extra": [3]})
        returned = validate_schema(df, "demo", required)
        self.assertIs(returned, df)  # returned unchanged for chaining

    def test_raises_with_missing_columns_listed(self) -> None:
        required = frozenset({"a", "b", "c"})
        df = pd.DataFrame({"a": [1]})
        with self.assertRaises(ValueError) as ctx:
            validate_schema(df, "demo", required)
        message = str(ctx.exception)
        self.assertIn("demo", message)
        self.assertIn("b", message)
        self.assertIn("c", message)

    def test_every_declared_raw_schema_is_self_consistent(self) -> None:
        # A frame built from exactly the required columns must validate.
        for name, required in REQUIRED_RAW_SCHEMAS.items():
            with self.subTest(schema=name):
                df = pd.DataFrame({col: [] for col in required})
                self.assertIs(validate_schema(df, name, required), df)

    def test_every_declared_processed_schema_is_self_consistent(self) -> None:
        for name, required in REQUIRED_PROCESSED_SCHEMAS.items():
            with self.subTest(schema=name):
                df = pd.DataFrame({col: [] for col in required})
                self.assertIs(validate_schema(df, name, required), df)


class TestLoggingSetup(unittest.TestCase):
    def setUp(self) -> None:
        self._root = logging.getLogger()
        self._saved_handlers = self._root.handlers[:]
        self._saved_level = self._root.level

    def tearDown(self) -> None:
        self._root.handlers = self._saved_handlers
        self._root.setLevel(self._saved_level)

    def test_configure_logging_is_idempotent(self) -> None:
        self._root.handlers = []
        configure_logging("INFO")
        first = list(self._root.handlers)
        self.assertTrue(first)
        configure_logging("INFO")  # second call must not add handlers
        self.assertEqual(self._root.handlers, first)

    def test_explicit_level_is_respected(self) -> None:
        self._root.handlers = []
        configure_logging("WARNING")
        self.assertEqual(self._root.level, logging.WARNING)

    def test_get_logger_returns_named_logger_and_configures(self) -> None:
        self._root.handlers = []
        logger = get_logger("revenue_quality.test")
        self.assertEqual(logger.name, "revenue_quality.test")
        self.assertTrue(self._root.handlers)


if __name__ == "__main__":
    unittest.main()

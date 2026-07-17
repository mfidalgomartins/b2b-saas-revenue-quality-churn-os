"""Configurable ingestion for governed production data extracts."""

from src.ingestion.contracts import IngestionContract, load_ingestion_contract

__all__ = ["IngestionContract", "load_ingestion_contract"]

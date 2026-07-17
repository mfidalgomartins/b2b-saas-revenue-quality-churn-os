# Governed real-data ingestion

The CSV adapter converts source extracts into the six canonical raw tables without changing downstream analytics code. It validates the complete snapshot in memory, applies keyed HMAC-SHA256 pseudonyms, removes configured PII, checks source and record freshness, and enforces primary-key and foreign-key integrity before publication.

## Operating contract

Each source must provide a JSON contract owned by the data producer and analytics owner. Relative paths are resolved beside the contract; table file paths cannot escape `source_dir`. Unknown configuration keys fail fast.

Required root fields:

- `version`: currently `1`.
- `source_name`: stable source-system identifier.
- `source_dir` and `output_dir`: source extract and canonical snapshot directories.
- `publication_allowed`: explicit data-governance decision. This is recorded in the manifest; it does not publish artifacts automatically.
- `anonymization`: `enabled`, secret-key environment variable, and digest length.
- `tables`: source-to-canonical mappings and table-level SLAs.
- `foreign_keys`: relationships checked after all tables pass their own contracts.

Minimal table example:

```json
{
  "version": 1,
  "source_name": "billing_monthly_export",
  "source_dir": "/secure/drop/billing",
  "output_dir": "../../data/raw",
  "publication_allowed": false,
  "anonymization": {
    "enabled": true,
    "key_env": "REVENUE_OS_HMAC_KEY",
    "digest_length": 20
  },
  "tables": {
    "customers": {
      "file": "customers.csv",
      "required_columns": ["customer_id", "signup_date", "region", "segment", "company_size", "industry", "acquisition_channel", "account_manager_id", "lifecycle_stage"],
      "primary_key": ["customer_id"],
      "parse_dates": ["signup_date"],
      "rename_columns": {"crm_account_id": "customer_id"},
      "drop_columns": ["company_name", "contact_email"],
      "anonymize_columns": {
        "customer_id": "customer",
        "account_manager_id": "account_manager"
      },
      "max_null_rate": {"segment": 0.01},
      "min_rows": 1,
      "max_file_age_hours": 36,
      "freshness_column": "signup_date",
      "max_record_age_days": 3650
    }
  },
  "foreign_keys": []
}
```

A production contract must define all tables and fields in `REQUIRED_RAW_SCHEMAS` from `src/io/contracts.py`; the example above is intentionally limited to one table to show the syntax.

## Runbook

Set a secret with at least 16 UTF-8 bytes and run the adapter:

```bash
export REVENUE_OS_HMAC_KEY='replace-with-a-secret-from-your-vault'
make ingest INGESTION_CONFIG=/secure/contracts/revenue_os.json
python -m src.pipeline.run_project_pipeline \
  --base-dir . \
  --skip-data-generation \
  --intervention-ledger /secure/ledgers/retention_assignment.csv \
  --skip-gate
```

The example assumes `publication_allowed=false`, so it builds confidential
decision artifacts but deliberately skips the publication gate. When the data
owner explicitly authorizes publication in the contract, omit `--skip-gate`;
validation check 24 reconciles every raw file to the manifest before release.
Real-data runs require a prospectively captured intervention ledger. The
pipeline refuses to create a retrospective random assignment after outcomes are
known; the built-in assignment generator is reserved for the synthetic demo.

The adapter writes canonical CSVs and `ingestion_manifest.json`. The manifest records row counts, checksums, freshness observations, contract checksum, anonymization state, and the publication decision. It contains no secret and exposes only configured source filenames, not absolute source paths.

## Failure semantics and governance

- Any schema, null-rate, key, freshness, or referential-integrity violation blocks the entire snapshot.
- Staged files are promoted only after every validation passes; the manifest is moved last and acts as the commit marker.
- A missing or short HMAC secret blocks ingestion. Use the same secret and namespace across related tables so pseudonymous keys remain joinable.
- Raw production extracts and secrets must remain outside version control. `publication_allowed=false` means generated analytics must not be pushed to public hosting or committed.
- Re-running the same contract, extracts, secret, and evaluation time produces byte-identical canonical CSVs and hashes.

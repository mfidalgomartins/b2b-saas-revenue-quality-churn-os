# Data & Artifact Contracts

## Purpose
Define non-negotiable integrity contracts that gate analytical publication.

## Raw Data Contracts
1. Primary keys are unique (`customer_id`, `subscription_id`, `invoice_id`, monthly composite key).
2. Signup chronology is coherent with subscription starts.
3. Binary flags are strictly `{0,1}`.
4. Invoice arithmetic is coherent:
   - `effective_revenue_adjustment_amount ~= discount_amount + collection_loss_amount` (2-cent tolerance)
   - `billed_mrr - effective_revenue_adjustment_amount ~= realized_mrr` (2-cent tolerance).
5. Status domains are controlled (`active/churned`, expected payment statuses).

## Processed Data Contracts
1. Account-level tables share the same customer universe.
2. Monthly analytical panel keys match raw monthly keys.
3. Score columns remain in `[0,100]`.
4. Tier labels remain in `{Low, Moderate, High, Critical}`.
5. Scenario summary and trajectory shape remain consistent.

## Artifact Contracts
1. Dashboard HTML exists and embeds JSON payload with governed keys:
   - `meta`, `official_kpis`, `filters`, `accounts`, `manager_panel`, `scenario_cards`, `risk_impact`, `chart_catalog`, `methodology`, `source_map`.
2. Release manifest includes metadata, coverage, counts, and validation summary.
3. Checksums file is generated and non-empty.
4. Validation summary includes governance readiness classification and readiness scale.

## Enforcement
- Local: `make qa`
- CI: `.github/workflows/qa.yml`
- Validation gate: `src/validation/check_validation_gate.py`

# Synthetic Data Design Blueprint

## Objective
Generate realistic B2B SaaS commercial data that supports revenue quality analysis, churn early warning, discount governance, and scenario forecasting.

## Simulation Horizon and Scale
- Time grain: monthly panel.
- Horizon: 36 months ending 2026-02-01.
- Scale: 4,500 customers.

## Output Tables and Grains
1. `customers` (one row per customer)
- Keys: `customer_id`
- Business dimensions: region, segment, company size, industry, acquisition channel, AM ownership, lifecycle stage.

2. `plans` (one row per commercial plan)
- Keys: `plan_id`
- Commercial attributes: tier, billing cycle, list MRR, included seats.

3. `subscriptions` (one row per customer-month active subscription snapshot)
- Keys: `subscription_id`
- Includes month-level contracted MRR, realized MRR, discount %, renewal flag, seats purchased, plan assignment.

4. `monthly_account_metrics` (one row per customer-month in panel)
- Keys: `customer_id`, `month`
- Leading indicators and revenue events: usage, support burden, NPS, payment delays, expansion, contraction, churn, renewal due.

5. `invoices` (one row per customer-month billed)
- Keys: `invoice_id`
- Billing/collection metrics: billed MRR, realized MRR, discount amount, payment status, days to pay.

6. `account_managers` (one row per account manager)
- Keys: `account_manager_id`
- Team assignment, region, tenure.

## Core Behavioral Logic
### 1) Customer composition
- Segments: SMB, Mid-Market, Enterprise with weighted distribution.
- Regions and industries are sampled with non-uniform business-like proportions.
- Acquisition channels are segment-dependent (e.g., enterprise over-indexes on enterprise sales and partners).

### 2) Plan and seat distribution
- Segment-specific plan mix (Enterprise skews to Pro/Enterprise annual plans; SMB skews to Basic/Growth).
- Seat counts follow segment-specific lognormal distributions.
- A small concentrated-account cohort gets seat multipliers to create mild revenue concentration.

### 3) Discount discipline and channel effects
- Base discount set by channel, then adjusted by segment, billing cycle, and customer quality.
- Paid media/outbound/partner cohorts have higher expected discounts than self-serve/content.
- Discounts evolve at renewal: healthy accounts can renew with lower discount; weak accounts get pressured discounts.

### 4) Health signal generation
Each active customer-month generates:
- `product_usage_score`: quality + growth + maturity ramp + seasonality + noise.
- `support_tickets`: increases with seats and poor product experience.
- `payment_delay_days`: worsens with low quality, high discount dependence, and ticket burden.
- `nps_score`: positively tied to usage and negatively to support burden and payment friction.

### 5) Expansion and contraction
- Expansion probability rises with high usage, high NPS, low payment delay, and renewal windows.
- Contraction probability rises with poor usage, payment stress, and elevated support burden.
- Plan upgrades/downgrades can happen when expansion/contraction is material.

### 6) Churn mechanism
- Baseline churn differs by segment (Enterprise lower than SMB).
- Churn risk increases with low usage, low NPS, high payment delay, high support burden, heavy discounting, and hidden-risk traits.
- Annual contracts have much lower mid-term churn and concentrated churn pressure at renewal.
- Renewal seasonality is injected with higher churn pressure in selected renewal months.

### 7) Special account archetypes
- Hidden-risk accounts: high current MRR but degrading health signals over time.
- Fragile expanders: discount-led expansion followed by elevated churn probability in the next 3-9 months.

## Data Quality and Consistency Constraints
- Unique IDs for all entity/event tables.
- `realized_mrr <= billed_mrr` and reflects commercial discount plus collection losses.
- Invoice components are explicit:
  - `discount_amount` (commercial discount only),
  - `collection_loss_amount` (collections/default haircut),
  - `effective_revenue_adjustment_amount` (sum of both components, capped at billed MRR).
- Subscription and invoice months are aligned.
- Customer lifecycle stage is updated from latest observed status/risk.

## Intended Analytical Discoveries
- Better retention in enterprise vs SMB.
- Higher discount intensity in selected channels.
- Detectable churn-risk concentration where usage/NPS/payment/support deteriorate.
- Distinct healthy vs fragile expansion cohorts.
- Mild but meaningful concentration risk in top accounts.

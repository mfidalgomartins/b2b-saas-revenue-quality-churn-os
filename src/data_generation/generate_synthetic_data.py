from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.data_generation.simulation import (
    CommercialState,
    build_billing,
    inactive_month_row,
    lifecycle_stage,
    simulate_churn,
    simulate_engagement,
    simulate_revenue_movement,
)
from src.io.logging_setup import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class GenerationConfig:
    n_customers: int = 4500
    months_history: int = 36
    seed: int = 42
    end_month: str = "2026-02-01"


def build_month_range(end_month: str, months_history: int) -> pd.DatetimeIndex:
    end_ts = pd.Timestamp(end_month)
    return pd.date_range(end=end_ts, periods=months_history, freq="MS")


def generate_account_managers(rng: np.random.Generator, n_customers: int) -> pd.DataFrame:
    n_managers = max(24, int(n_customers / 110))
    manager_ids = [f"AM{idx:04d}" for idx in range(1, n_managers + 1)]

    teams = [
        "SMB Pod A",
        "SMB Pod B",
        "MM East",
        "MM West",
        "Enterprise Strategic",
        "Enterprise Global",
    ]
    regions = ["North America", "EMEA", "APAC", "LATAM"]

    df = pd.DataFrame(
        {
            "account_manager_id": manager_ids,
            "team": rng.choice(teams, size=n_managers, p=[0.2, 0.2, 0.18, 0.18, 0.14, 0.1]),
            "region": rng.choice(regions, size=n_managers, p=[0.42, 0.3, 0.18, 0.1]),
            "tenure_months": np.clip(np.round(rng.gamma(shape=3.2, scale=12, size=n_managers)).astype(int), 3, 120),
        }
    )
    return df


def generate_plans() -> pd.DataFrame:
    rows = [
        ("P1", "Launch Monthly", "Basic", "monthly", 280.0, 5),
        ("P2", "Launch Annual", "Basic", "annual", 250.0, 5),
        ("P3", "Growth Monthly", "Growth", "monthly", 950.0, 20),
        ("P4", "Growth Annual", "Growth", "annual", 830.0, 20),
        ("P5", "Scale Monthly", "Pro", "monthly", 2600.0, 60),
        ("P6", "Scale Annual", "Pro", "annual", 2250.0, 60),
        ("P7", "Enterprise Flex", "Enterprise", "monthly", 7200.0, 180),
        ("P8", "Enterprise Commit", "Enterprise", "annual", 6400.0, 180),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "plan_id",
            "plan_name",
            "plan_tier",
            "billing_cycle",
            "list_mrr",
            "included_seats",
        ],
    )


def _sample_with_map(rng: np.random.Generator, key: str, options_map: dict[str, tuple[list[str], list[float]]]) -> str:
    values, probs = options_map[key]
    return str(rng.choice(values, p=probs))


def generate_customers(
    rng: np.random.Generator,
    n_customers: int,
    months: pd.DatetimeIndex,
    account_managers: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    customer_ids = [f"CUST{idx:06d}" for idx in range(1, n_customers + 1)]

    regions = rng.choice(["North America", "EMEA", "APAC", "LATAM"], size=n_customers, p=[0.42, 0.3, 0.18, 0.1])
    segments = rng.choice(["SMB", "Mid-Market", "Enterprise"], size=n_customers, p=[0.58, 0.28, 0.14])

    size_map = {
        "SMB": (["1-20", "21-50", "51-200"], [0.56, 0.31, 0.13]),
        "Mid-Market": (["51-200", "201-500", "501-1000"], [0.33, 0.42, 0.25]),
        "Enterprise": (["501-1000", "1001-5000", "5000+"], [0.28, 0.46, 0.26]),
    }
    company_sizes = [_sample_with_map(rng, seg, size_map) for seg in segments]

    industries = rng.choice(
        ["SaaS", "FinTech", "Healthcare", "Manufacturing", "Retail", "Education", "Logistics", "Media"],
        size=n_customers,
        p=[0.18, 0.11, 0.12, 0.16, 0.14, 0.1, 0.1, 0.09],
    )

    channel_map = {
        "SMB": (
            ["self_serve", "paid_media", "content_marketing", "partner_referral", "outbound_sdr"],
            [0.36, 0.24, 0.17, 0.12, 0.11],
        ),
        "Mid-Market": (
            ["outbound_sdr", "partner_referral", "content_marketing", "paid_media", "enterprise_sales"],
            [0.29, 0.24, 0.2, 0.14, 0.13],
        ),
        "Enterprise": (
            ["enterprise_sales", "partner_referral", "outbound_sdr", "content_marketing"],
            [0.49, 0.28, 0.17, 0.06],
        ),
    }
    acquisition_channels = [_sample_with_map(rng, seg, channel_map) for seg in segments]

    start_month = months.min()
    end_month = months.max()
    legacy_start = start_month - pd.DateOffset(months=20)

    signup_dates = []
    for _ in range(n_customers):
        is_legacy = rng.random() < 0.34
        if is_legacy:
            dt = legacy_start + pd.Timedelta(days=int(rng.integers(0, (start_month - legacy_start).days)))
        else:
            dt = start_month + pd.Timedelta(days=int(rng.integers(0, (end_month - start_month).days + 1)))
        # Store at month-start so signup chronology is coherent with monthly-grain subscription starts.
        signup_dates.append(pd.Timestamp(dt).replace(day=1).normalize())

    am_region_map: dict[str, list[str]] = {
        region: account_managers.loc[account_managers["region"] == region, "account_manager_id"].tolist()
        for region in ["North America", "EMEA", "APAC", "LATAM"]
    }
    fallback_ams = account_managers["account_manager_id"].tolist()

    account_manager_ids = []
    for region in regions:
        regional_pool = am_region_map.get(region, [])
        if regional_pool:
            account_manager_ids.append(str(rng.choice(regional_pool)))
        else:
            account_manager_ids.append(str(rng.choice(fallback_ams)))

    quality_alpha = {"SMB": (2.4, 2.6), "Mid-Market": (3.2, 2.2), "Enterprise": (3.8, 1.9)}
    growth_alpha = {"SMB": (2.0, 2.8), "Mid-Market": (2.8, 2.1), "Enterprise": (3.4, 1.9)}

    base_quality = np.zeros(n_customers)
    growth_potential = np.zeros(n_customers)
    hidden_risk = np.zeros(n_customers, dtype=int)
    fragile_expander = np.zeros(n_customers, dtype=int)
    concentration_flag = np.zeros(n_customers, dtype=int)

    for i, seg in enumerate(segments):
        qa, qb = quality_alpha[seg]
        ga, gb = growth_alpha[seg]
        base_quality[i] = rng.beta(qa, qb)
        growth_potential[i] = rng.beta(ga, gb)

        hidden_risk[i] = int(rng.random() < {"SMB": 0.09, "Mid-Market": 0.08, "Enterprise": 0.07}[seg])
        fragile_expander[i] = int(rng.random() < {"SMB": 0.12, "Mid-Market": 0.1, "Enterprise": 0.08}[seg])
        concentration_flag[i] = int(rng.random() < {"SMB": 0.005, "Mid-Market": 0.02, "Enterprise": 0.12}[seg])

    customers = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "signup_date": pd.to_datetime(signup_dates),
            "region": regions,
            "segment": segments,
            "company_size": company_sizes,
            "industry": industries,
            "acquisition_channel": acquisition_channels,
            "account_manager_id": account_manager_ids,
            "lifecycle_stage": "Active",
        }
    )

    latent = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "base_quality": base_quality,
            "growth_potential": growth_potential,
            "hidden_risk": hidden_risk,
            "fragile_expander": fragile_expander,
            "concentration_flag": concentration_flag,
        }
    )
    return customers, latent


def _pick_initial_plan(rng: np.random.Generator, segment: str) -> str:
    if segment == "SMB":
        ids = ["P1", "P2", "P3", "P4"]
        probs = [0.44, 0.22, 0.24, 0.1]
    elif segment == "Mid-Market":
        ids = ["P3", "P4", "P5", "P6"]
        probs = [0.29, 0.27, 0.25, 0.19]
    else:
        ids = ["P5", "P6", "P7", "P8"]
        probs = [0.11, 0.17, 0.26, 0.46]
    return str(rng.choice(ids, p=probs))


def _initial_seats(
    rng: np.random.Generator,
    segment: str,
    included_seats: int,
    concentration_flag: int,
) -> int:
    if segment == "SMB":
        seats = int(np.clip(np.round(rng.lognormal(mean=2.1, sigma=0.45)), 2, 70))
    elif segment == "Mid-Market":
        seats = int(np.clip(np.round(rng.lognormal(mean=3.4, sigma=0.5)), 15, 350))
    else:
        seats = int(np.clip(np.round(rng.lognormal(mean=4.8, sigma=0.55)), 70, 1800))

    seats = max(seats, int(included_seats * rng.uniform(0.8, 1.35)))

    if concentration_flag == 1:
        seats = int(seats * rng.uniform(1.8, 4.8))
    return int(np.clip(seats, 2, 3500))


def _contracted_mrr(list_mrr: float, included_seats: int, seats: int) -> float:
    seat_ratio = max(seats / included_seats, 0.25)
    if seat_ratio <= 1:
        multiplier = 0.75 + 0.25 * seat_ratio
    else:
        multiplier = 1 + 0.85 * (seat_ratio - 1)
    return float(max(90.0, list_mrr * multiplier))


def _base_discount(
    rng: np.random.Generator,
    channel: str,
    segment: str,
    billing_cycle: str,
    quality: float,
) -> float:
    channel_base = {
        "self_serve": 0.04,
        "content_marketing": 0.07,
        "outbound_sdr": 0.15,
        "paid_media": 0.21,
        "partner_referral": 0.16,
        "enterprise_sales": 0.19,
    }
    segment_adj = {"SMB": 0.02, "Mid-Market": 0.01, "Enterprise": 0.0}
    billing_adj = 0.03 if billing_cycle == "annual" else 0.0
    quality_adj = 0.09 * (1 - quality)

    discount = channel_base.get(channel, 0.1) + segment_adj.get(segment, 0.0) + billing_adj + quality_adj
    discount += float(rng.normal(0, 0.025))
    return float(np.clip(discount, 0.0, 0.55))


def _monthly_term_length(billing_cycle: str) -> int:
    return 12 if billing_cycle == "annual" else 3


def simulate_subscription_and_metrics(
    rng: np.random.Generator,
    customers: pd.DataFrame,
    latent: pd.DataFrame,
    plans: pd.DataFrame,
    months: pd.DatetimeIndex,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
    plan_lookup = plans.set_index("plan_id").to_dict("index")

    subscriptions_rows = []
    monthly_rows = []
    invoice_rows = []
    lifecycle_updates = {}

    month_start = months.min()
    month_end = months.max()

    latent_map = latent.set_index("customer_id").to_dict("index")

    for _, cust in customers.iterrows():
        cid = cust["customer_id"]
        segment = cust["segment"]
        channel = cust["acquisition_channel"]
        signup_month = pd.Timestamp(cust["signup_date"]).replace(day=1)
        activation_month = max(signup_month, month_start)

        lat = latent_map[cid]
        quality = float(lat["base_quality"])
        growth = float(lat["growth_potential"])
        hidden_risk = int(lat["hidden_risk"])
        fragile = int(lat["fragile_expander"])
        concentration = int(lat["concentration_flag"])

        plan_id = _pick_initial_plan(rng, segment)
        plan_info = plan_lookup[plan_id]
        billing_cycle = str(plan_info["billing_cycle"])
        current_tier = str(plan_info["plan_tier"])
        term_len = _monthly_term_length(billing_cycle)

        seats = _initial_seats(rng, segment, int(plan_info["included_seats"]), concentration)
        current_contracted = _contracted_mrr(float(plan_info["list_mrr"]), int(plan_info["included_seats"]), seats)
        current_discount = _base_discount(rng, channel, segment, billing_cycle, quality)
        state = CommercialState(
            plan_id=plan_id,
            billing_cycle=billing_cycle,
            current_tier=current_tier,
            seats=seats,
            current_contracted=current_contracted,
            current_discount=current_discount,
            contract_anchor=activation_month,
        )

        for month in months:
            if month < activation_month:
                continue

            if state.churned:
                monthly_rows.append(inactive_month_row(str(cid), month))
                continue

            month_index = int((month.year - activation_month.year) * 12 + (month.month - activation_month.month))
            contract_month_index = int(
                (month.year - state.contract_anchor.year) * 12 + (month.month - state.contract_anchor.month)
            )
            renewal_due_flag = int(((contract_month_index + 1) % term_len) == 0)

            signals = simulate_engagement(
                rng,
                month,
                activation_month,
                state,
                str(segment),
                quality,
                growth,
                hidden_risk,
            )

            movement = simulate_revenue_movement(
                rng,
                month,
                state,
                plans,
                str(segment),
                fragile,
                signals,
                renewal_due_flag,
            )

            churn_flag = simulate_churn(
                rng,
                month,
                state,
                str(segment),
                hidden_risk,
                signals,
                renewal_due_flag,
            )
            billing = build_billing(state, signals.payment_delay)

            subscriptions_rows.append(
                {
                    "subscription_id": f"SUB-{cid}-{month.strftime('%Y%m')}",
                    "customer_id": cid,
                    "plan_id": state.plan_id,
                    "subscription_start_date": month,
                    "subscription_end_date": month + pd.offsets.MonthEnd(1),
                    "status": "churned" if churn_flag else "active",
                    "seats_purchased": state.seats,
                    "contracted_mrr": round(billing.billed_mrr, 2),
                    "realized_mrr": round(billing.realized_mrr, 2),
                    "discount_pct": round(state.current_discount, 4),
                    "renewal_flag": renewal_due_flag,
                }
            )

            invoice_rows.append(
                {
                    "invoice_id": f"INV-{cid}-{month.strftime('%Y%m')}",
                    "customer_id": cid,
                    "invoice_month": month,
                    "billed_mrr": round(billing.billed_mrr, 2),
                    "realized_mrr": round(billing.realized_mrr, 2),
                    "discount_amount": round(billing.commercial_discount_amount, 2),
                    "collection_loss_amount": round(billing.collection_loss_amount, 2),
                    "effective_revenue_adjustment_amount": round(billing.effective_revenue_adjustment_amount, 2),
                    "payment_status": billing.payment_status,
                    "days_to_pay": signals.payment_delay,
                }
            )

            monthly_rows.append(
                {
                    "customer_id": cid,
                    "month": month,
                    "active_flag": 1,
                    "seats_active": signals.seats_active,
                    "product_usage_score": round(signals.usage, 2),
                    "support_tickets": signals.support_tickets,
                    "nps_score": round(signals.nps, 2),
                    "payment_delay_days": signals.payment_delay,
                    "expansion_mrr": round(movement.expansion_mrr, 2),
                    "contraction_mrr": round(movement.contraction_mrr, 2),
                    "churn_flag": churn_flag,
                    "downgrade_flag": movement.downgrade_flag,
                    "renewal_due_flag": renewal_due_flag,
                }
            )

            if churn_flag:
                state.churned = True
            elif renewal_due_flag == 1:
                state.contract_anchor = month + pd.DateOffset(months=1)

            if month == month_end or churn_flag:
                lifecycle_updates[cid] = lifecycle_stage(
                    state.churned,
                    month_index,
                    renewal_due_flag,
                    signals,
                )

    subscriptions = pd.DataFrame(subscriptions_rows)
    monthly_metrics = pd.DataFrame(monthly_rows)
    invoices = pd.DataFrame(invoice_rows)
    lifecycle_series = pd.Series(lifecycle_updates, name="lifecycle_stage")

    return subscriptions, monthly_metrics, invoices, lifecycle_series


def save_tables(
    output_dir: Path,
    customers: pd.DataFrame,
    plans: pd.DataFrame,
    subscriptions: pd.DataFrame,
    monthly_metrics: pd.DataFrame,
    invoices: pd.DataFrame,
    account_managers: pd.DataFrame,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "ingestion_manifest.json").unlink(missing_ok=True)

    tables = {
        "customers.csv": customers,
        "plans.csv": plans,
        "subscriptions.csv": subscriptions,
        "monthly_account_metrics.csv": monthly_metrics,
        "invoices.csv": invoices,
        "account_managers.csv": account_managers,
    }

    for filename, df in tables.items():
        df.to_csv(output_dir / filename, index=False)


def write_synthetic_manifest(output_dir: Path, config: GenerationConfig) -> None:
    """Write deterministic provenance for the generated canonical snapshot."""
    table_names = (
        "account_managers.csv",
        "customers.csv",
        "invoices.csv",
        "monthly_account_metrics.csv",
        "plans.csv",
        "subscriptions.csv",
    )
    tables: dict[str, dict[str, int | str]] = {}
    for filename in table_names:
        path = output_dir / filename
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        with path.open(encoding="utf-8") as handle:
            rows = sum(1 for _ in handle) - 1
        tables[filename.removesuffix(".csv")] = {
            "output_file": filename,
            "output_sha256": digest,
            "rows": rows,
        }

    manifest = {
        "manifest_version": 1,
        "source_type": "synthetic",
        "generator": "src.data_generation.generate_synthetic_data",
        "seed": config.seed,
        "n_customers": config.n_customers,
        "months_history": config.months_history,
        "end_month": config.end_month,
        "status": "PASS",
        "tables": tables,
    }
    (output_dir / "synthetic_data_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_generation_note(
    output_path: Path,
    config: GenerationConfig,
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    monthly_metrics: pd.DataFrame,
    invoices: pd.DataFrame,
) -> None:
    churned_customers = int(monthly_metrics.loc[monthly_metrics["churn_flag"] == 1, "customer_id"].nunique())
    avg_discount = float(subscriptions["discount_pct"].mean())
    top10_share = float(
        subscriptions.groupby("customer_id", as_index=False)["contracted_mrr"]
        .max()
        .nlargest(10, "contracted_mrr")["contracted_mrr"]
        .sum()
        / subscriptions.groupby("customer_id", as_index=False)["contracted_mrr"].max()["contracted_mrr"].sum()
    )
    paid_mix = invoices["payment_status"].value_counts(normalize=True).round(3).to_dict()

    note = f"""## Latest Generation Snapshot

### Scope
- Customers: {len(customers):,}
- History length: {config.months_history} monthly periods ending {config.end_month}
- Subscription-month snapshots: {len(subscriptions):,}
- Monthly account metric rows: {len(monthly_metrics):,}
- Invoice rows: {len(invoices):,}

### Embedded Business Logic
- Segment-specific retention behavior: Enterprise has lower baseline churn than SMB.
- Discount behavior varies by acquisition channel, billing cycle, and customer quality.
- Churn probability increases when usage declines, NPS falls, payment delays rise, support burden rises, and discounting is heavy.
- Healthy expansions happen for high-usage/high-NPS/low-delay accounts.
- Fragile expansion path is explicitly simulated: some accounts expand under deep discounts then face elevated churn risk 3-9 months later.
- Hidden risk accounts are simulated with high current MRR but degrading leading indicators.
- Revenue concentration is introduced via a small set of high-seat enterprise/concentrated accounts.
- Renewal seasonality is encoded through renewal probabilities and churn pressure around renewal windows.

### Quick Diagnostics
- Unique churned customers in window: {churned_customers:,}
- Average discount_pct in subscriptions: {avg_discount:.2%}
- Top-10 account concentration (share of peak contracted MRR): {top10_share:.2%}
- Payment status mix: {paid_mix}

### Intended Patterns for Downstream Analysis
- Better GRR/NRR in enterprise cohorts vs SMB cohorts.
- Higher discount intensity in paid_media and outbound-led acquisition.
- At-risk ARR concentrated where usage/NPS trend down and delays/support worsen.
- Expansion quality split: healthy expansion cohorts retain better than high-discount expansion cohorts.
- High-MRR hidden-risk account watchlist should surface when combining ARR exposure with forward risk.
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        existing = output_path.read_text(encoding="utf-8")
        marker = "## Latest Generation Snapshot"
        if marker in existing:
            prefix = existing.split(marker)[0].rstrip()
            updated = f"{prefix}\n\n{note.strip()}\n"
        else:
            updated = f"{existing.rstrip()}\n\n{note.strip()}\n"
        output_path.write_text(updated, encoding="utf-8")
    else:
        output_path.write_text(note, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic B2B SaaS revenue quality datasets.")
    parser.add_argument("--n-customers", type=int, default=4500)
    parser.add_argument("--months-history", type=int, default=36)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--end-month", type=str, default="2026-02-01")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Directory where CSV files will be written.",
    )
    parser.add_argument(
        "--note-path",
        type=str,
        default="docs/core/synthetic_data.md",
        help="Path for the synthetic data design and generation markdown file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = GenerationConfig(
        n_customers=args.n_customers,
        months_history=args.months_history,
        seed=args.seed,
        end_month=args.end_month,
    )

    rng = np.random.default_rng(config.seed)
    months = build_month_range(config.end_month, config.months_history)

    account_managers = generate_account_managers(rng=rng, n_customers=config.n_customers)
    plans = generate_plans()
    customers, latent = generate_customers(
        rng=rng,
        n_customers=config.n_customers,
        months=months,
        account_managers=account_managers,
    )

    subscriptions, monthly_metrics, invoices, lifecycle_updates = simulate_subscription_and_metrics(
        rng=rng,
        customers=customers,
        latent=latent,
        plans=plans,
        months=months,
    )

    customers = customers.copy()
    customers["lifecycle_stage"] = customers["customer_id"].map(lifecycle_updates).fillna("Onboarding")

    output_dir = Path(args.output_dir)
    save_tables(
        output_dir=output_dir,
        customers=customers,
        plans=plans,
        subscriptions=subscriptions,
        monthly_metrics=monthly_metrics,
        invoices=invoices,
        account_managers=account_managers,
    )
    write_synthetic_manifest(output_dir, config)

    build_generation_note(
        output_path=Path(args.note_path),
        config=config,
        customers=customers,
        subscriptions=subscriptions,
        monthly_metrics=monthly_metrics,
        invoices=invoices,
    )

    log.info("Synthetic data generation complete.")
    log.info("Output directory: %s", output_dir.resolve())
    log.info("customers: %s", f"{len(customers):,}")
    log.info("plans: %s", f"{len(plans):,}")
    log.info("subscriptions: %s", f"{len(subscriptions):,}")
    log.info("monthly_account_metrics: %s", f"{len(monthly_metrics):,}")
    log.info("invoices: %s", f"{len(invoices):,}")


if __name__ == "__main__":
    main()

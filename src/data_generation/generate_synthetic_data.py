from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


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


def _sample_with_map(rng: np.random.Generator, key: str, options_map: Dict[str, Tuple[List[str], List[float]]]) -> str:
    values, probs = options_map[key]
    return str(rng.choice(values, p=probs))


def generate_customers(
    rng: np.random.Generator,
    n_customers: int,
    months: pd.DatetimeIndex,
    account_managers: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

    am_region_map: Dict[str, List[str]] = {
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


def _tier_rank(tier: str) -> int:
    order = {"Basic": 0, "Growth": 1, "Pro": 2, "Enterprise": 3}
    return order[tier]


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


def _plan_id_for_tier_cycle(plans: pd.DataFrame, target_tier: str, cycle: str) -> str:
    row = plans[(plans["plan_tier"] == target_tier) & (plans["billing_cycle"] == cycle)].iloc[0]
    return str(row["plan_id"])


def simulate_subscription_and_metrics(
    rng: np.random.Generator,
    customers: pd.DataFrame,
    latent: pd.DataFrame,
    plans: pd.DataFrame,
    months: pd.DatetimeIndex,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
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

        contract_anchor = activation_month
        churned = False
        fragile_expansion_month: pd.Timestamp | None = None

        for month in months:
            if month < activation_month:
                continue

            if churned:
                monthly_rows.append(
                    {
                        "customer_id": cid,
                        "month": month,
                        "active_flag": 0,
                        "seats_active": 0,
                        "product_usage_score": np.nan,
                        "support_tickets": 0,
                        "nps_score": np.nan,
                        "payment_delay_days": np.nan,
                        "expansion_mrr": 0.0,
                        "contraction_mrr": 0.0,
                        "churn_flag": 0,
                        "downgrade_flag": 0,
                        "renewal_due_flag": 0,
                    }
                )
                continue

            month_index = int((month.year - activation_month.year) * 12 + (month.month - activation_month.month))
            contract_month_index = int((month.year - contract_anchor.year) * 12 + (month.month - contract_anchor.month))
            renewal_due_flag = int(((contract_month_index + 1) % term_len) == 0)

            seasonality = 4.0 * np.sin((month.month / 12.0) * 2 * np.pi)
            usage = 35 + 32 * quality + 21 * growth + 5 * np.log1p(month_index + 1) + seasonality
            usage += float(rng.normal(0, 7.5))

            if hidden_risk and month_index > 10:
                usage -= 1.1 * (month_index - 10)
            if fragile_expansion_month is not None:
                since_fragile = int((month.year - fragile_expansion_month.year) * 12 + (month.month - fragile_expansion_month.month))
                if since_fragile >= 2:
                    usage -= 2.3 * since_fragile

            usage = float(np.clip(usage, 5, 100))

            seats_active = int(np.clip(np.round(seats * (0.56 + usage / 125 + rng.normal(0, 0.05))), 1, seats))
            support_lambda = max(0.35, 0.35 + seats_active / 55 + (72 - usage) / 24 + 1.7 * (1 - quality))
            support_tickets = int(min(35, rng.poisson(support_lambda)))

            delay_mean = 3.5 + 11 * (1 - quality) + 9 * max(0.0, current_discount - 0.18)
            delay_mean += 0.8 * max(0, support_tickets - 5)
            delay_mean += {"SMB": 5.0, "Mid-Market": 2.5, "Enterprise": 1.0}[segment]

            if hidden_risk and month_index > 12:
                delay_mean += 4.5
            payment_delay = int(np.clip(np.round(rng.normal(delay_mean, 5.8)), 0, 95))

            nps = 1.9 * (usage - 50) - 1.8 * max(0, support_tickets - 6) - 0.45 * payment_delay
            nps += float(rng.normal(0, 11.5))
            nps = float(np.clip(nps, -100, 100))

            healthy_signal = (usage / 100) + max(0.0, nps) / 120 - (payment_delay / 95) - support_tickets / 40

            base_expand_prob = {"SMB": 0.014, "Mid-Market": 0.024, "Enterprise": 0.031}[segment]
            expansion_prob = base_expand_prob
            if usage > 70 and nps > 20 and payment_delay < 14:
                expansion_prob += 0.03
            if renewal_due_flag:
                expansion_prob += 0.018
            if month_index < 3:
                expansion_prob *= 0.6

            base_contr_prob = {"SMB": 0.011, "Mid-Market": 0.009, "Enterprise": 0.007}[segment]
            contraction_prob = base_contr_prob
            if usage < 48:
                contraction_prob += 0.026
            if payment_delay > 20:
                contraction_prob += 0.02
            if support_tickets > 8:
                contraction_prob += 0.011
            if renewal_due_flag:
                contraction_prob += 0.012

            force_fragile_expansion = False
            if fragile and fragile_expansion_month is None and 4 <= month_index <= 16:
                if rng.random() < 0.065:
                    force_fragile_expansion = True

            expansion_mrr = 0.0
            contraction_mrr = 0.0
            downgrade_flag = 0

            do_expansion = force_fragile_expansion or (rng.random() < np.clip(expansion_prob, 0, 0.55))
            do_contraction = (rng.random() < np.clip(contraction_prob, 0, 0.55)) and not do_expansion

            if do_expansion:
                if force_fragile_expansion:
                    expansion_pct = float(rng.uniform(0.16, 0.4))
                    current_discount = float(max(current_discount, rng.uniform(0.28, 0.47)))
                    fragile_expansion_month = month
                else:
                    expansion_pct = float(rng.uniform(0.05, 0.24))
                    current_discount = float(np.clip(current_discount + rng.normal(-0.005, 0.012), 0.0, 0.5))

                expansion_mrr = current_contracted * expansion_pct
                current_contracted += expansion_mrr

                added_seats = int(max(1, np.round(seats * expansion_pct * rng.uniform(0.4, 0.85))))
                seats += added_seats

                if expansion_pct > 0.22 and _tier_rank(current_tier) < 3 and rng.random() < 0.34:
                    new_tier = ["Basic", "Growth", "Pro", "Enterprise"][_tier_rank(current_tier) + 1]
                    plan_id = _plan_id_for_tier_cycle(plans, new_tier, billing_cycle)
                    plan_info = plan_lookup[plan_id]
                    current_tier = new_tier

            elif do_contraction:
                contraction_pct = float(rng.uniform(0.06, 0.28))
                contraction_mrr = current_contracted * contraction_pct
                current_contracted = max(85.0, current_contracted - contraction_mrr)
                seats = max(2, int(np.round(seats * (1 - contraction_pct * rng.uniform(0.55, 0.95)))))
                downgrade_flag = 1

                if contraction_pct > 0.2 and _tier_rank(current_tier) > 0 and rng.random() < 0.37:
                    new_tier = ["Basic", "Growth", "Pro", "Enterprise"][_tier_rank(current_tier) - 1]
                    plan_id = _plan_id_for_tier_cycle(plans, new_tier, billing_cycle)
                    plan_info = plan_lookup[plan_id]
                    current_tier = new_tier

            if renewal_due_flag and not do_expansion:
                if healthy_signal > 0.55:
                    current_discount = float(np.clip(current_discount - rng.uniform(0.0, 0.02), 0.0, 0.5))
                elif healthy_signal < 0.1:
                    current_discount = float(np.clip(current_discount + rng.uniform(0.0, 0.03), 0.0, 0.55))

            base_churn = {"SMB": 0.0105, "Mid-Market": 0.0068, "Enterprise": 0.0038}[segment]
            churn_prob = base_churn

            if usage < 35:
                churn_prob += 0.026
            elif usage < 48:
                churn_prob += 0.013

            if nps < -15:
                churn_prob += 0.018
            elif nps < 5:
                churn_prob += 0.009

            if payment_delay > 45:
                churn_prob += 0.032
            elif payment_delay > 20:
                churn_prob += 0.016

            if support_tickets > 10:
                churn_prob += 0.012
            if current_discount > 0.32:
                churn_prob += 0.009
            if hidden_risk and month_index > 12:
                churn_prob += 0.011

            if fragile_expansion_month is not None:
                since_fragile = int((month.year - fragile_expansion_month.year) * 12 + (month.month - fragile_expansion_month.month))
                if 3 <= since_fragile <= 9:
                    churn_prob += 0.03

            if usage > 75 and nps > 30 and payment_delay < 10:
                churn_prob -= 0.007

            if billing_cycle == "annual" and renewal_due_flag == 0:
                churn_prob *= 0.22

            if renewal_due_flag == 1 and month.month in (1, 7):
                churn_prob += 0.004

            churn_prob = float(np.clip(churn_prob, 0.0005, 0.45))
            churn_flag = int(rng.random() < churn_prob)

            billed_mrr = float(current_contracted)
            commercial_discount_amount = float(billed_mrr * current_discount)

            if payment_delay <= 8:
                payment_status = "paid_on_time"
            elif payment_delay <= 30:
                payment_status = "paid_late"
            elif payment_delay <= 60:
                payment_status = "overdue"
            else:
                payment_status = "defaulted"

            collection_loss_amount = 0.0
            if payment_status == "overdue":
                collection_loss_amount = 0.08 * billed_mrr
            elif payment_status == "defaulted":
                collection_loss_amount = 0.45 * billed_mrr

            # Keep invoice semantics explicit:
            # - discount_amount = commercial discount only
            # - collection_loss_amount = collections/default haircut
            # - effective_revenue_adjustment_amount = commercial + collection components
            effective_revenue_adjustment_amount = float(
                min(billed_mrr, commercial_discount_amount + collection_loss_amount)
            )
            realized_mrr = float(max(0.0, billed_mrr - effective_revenue_adjustment_amount))

            subscriptions_rows.append(
                {
                    "subscription_id": f"SUB-{cid}-{month.strftime('%Y%m')}",
                    "customer_id": cid,
                    "plan_id": plan_id,
                    "subscription_start_date": month,
                    "subscription_end_date": month + pd.offsets.MonthEnd(1),
                    "status": "churned" if churn_flag else "active",
                    "seats_purchased": seats,
                    "contracted_mrr": round(billed_mrr, 2),
                    "realized_mrr": round(realized_mrr, 2),
                    "discount_pct": round(current_discount, 4),
                    "renewal_flag": renewal_due_flag,
                }
            )

            invoice_rows.append(
                {
                    "invoice_id": f"INV-{cid}-{month.strftime('%Y%m')}",
                    "customer_id": cid,
                    "invoice_month": month,
                    "billed_mrr": round(billed_mrr, 2),
                    "realized_mrr": round(realized_mrr, 2),
                    "discount_amount": round(commercial_discount_amount, 2),
                    "collection_loss_amount": round(collection_loss_amount, 2),
                    "effective_revenue_adjustment_amount": round(effective_revenue_adjustment_amount, 2),
                    "payment_status": payment_status,
                    "days_to_pay": payment_delay,
                }
            )

            monthly_rows.append(
                {
                    "customer_id": cid,
                    "month": month,
                    "active_flag": 1,
                    "seats_active": seats_active,
                    "product_usage_score": round(usage, 2),
                    "support_tickets": support_tickets,
                    "nps_score": round(nps, 2),
                    "payment_delay_days": payment_delay,
                    "expansion_mrr": round(expansion_mrr, 2),
                    "contraction_mrr": round(contraction_mrr, 2),
                    "churn_flag": churn_flag,
                    "downgrade_flag": downgrade_flag,
                    "renewal_due_flag": renewal_due_flag,
                }
            )

            if churn_flag:
                churned = True
            else:
                if renewal_due_flag == 1:
                    contract_anchor = month + pd.DateOffset(months=1)

            if month == month_end or churn_flag:
                if churned:
                    lifecycle_updates[cid] = "Churned"
                else:
                    age = month_index
                    risk_signal = (1 - usage / 100) + max(0, -nps) / 120 + payment_delay / 95 + support_tickets / 35
                    if age <= 3:
                        lifecycle_updates[cid] = "Onboarding"
                    elif renewal_due_flag == 1:
                        lifecycle_updates[cid] = "Renewing Soon"
                    elif risk_signal > 1.45:
                        lifecycle_updates[cid] = "At Risk"
                    else:
                        lifecycle_updates[cid] = "Active"

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


def build_generation_note(
    output_path: Path,
    config: GenerationConfig,
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    monthly_metrics: pd.DataFrame,
    invoices: pd.DataFrame,
) -> None:
    churned_customers = int(
        monthly_metrics.loc[monthly_metrics["churn_flag"] == 1, "customer_id"].nunique()
    )
    avg_discount = float(subscriptions["discount_pct"].mean())
    top10_share = float(
        subscriptions.groupby("customer_id", as_index=False)["contracted_mrr"].max().nlargest(10, "contracted_mrr")["contracted_mrr"].sum()
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
        existing = output_path.read_text()
        marker = "## Latest Generation Snapshot"
        if marker in existing:
            prefix = existing.split(marker)[0].rstrip()
            updated = f"{prefix}\n\n{note.strip()}\n"
        else:
            updated = f"{existing.rstrip()}\n\n{note.strip()}\n"
        output_path.write_text(updated)
    else:
        output_path.write_text(note)


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

    build_generation_note(
        output_path=Path(args.note_path),
        config=config,
        customers=customers,
        subscriptions=subscriptions,
        monthly_metrics=monthly_metrics,
        invoices=invoices,
    )

    print("Synthetic data generation complete.")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"customers: {len(customers):,}")
    print(f"plans: {len(plans):,}")
    print(f"subscriptions: {len(subscriptions):,}")
    print(f"monthly_account_metrics: {len(monthly_metrics):,}")
    print(f"invoices: {len(invoices):,}")


if __name__ == "__main__":
    main()

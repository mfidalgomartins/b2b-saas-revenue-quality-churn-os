"""Domain steps for the monthly customer simulation.

The public generator owns orchestration and output ordering. This module owns
the state transitions for engagement, revenue movements, churn and billing.
Keeping those transitions separate makes the model reviewable without changing
the seeded RNG call order.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class CommercialState:
    plan_id: str
    billing_cycle: str
    current_tier: str
    seats: int
    current_contracted: float
    current_discount: float
    contract_anchor: pd.Timestamp
    churned: bool = False
    fragile_expansion_month: pd.Timestamp | None = None


@dataclass(frozen=True)
class EngagementSignals:
    month_index: int
    usage: float
    seats_active: int
    support_tickets: int
    payment_delay: int
    nps: float
    healthy_signal: float


@dataclass(frozen=True)
class RevenueMovement:
    expansion_mrr: float
    contraction_mrr: float
    downgrade_flag: int
    did_expand: bool


@dataclass(frozen=True)
class BillingResult:
    billed_mrr: float
    commercial_discount_amount: float
    collection_loss_amount: float
    effective_revenue_adjustment_amount: float
    realized_mrr: float
    payment_status: str


def inactive_month_row(customer_id: str, month: pd.Timestamp) -> dict[str, object]:
    return {
        "customer_id": customer_id,
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


def simulate_engagement(
    rng: np.random.Generator,
    month: pd.Timestamp,
    activation_month: pd.Timestamp,
    state: CommercialState,
    segment: str,
    quality: float,
    growth: float,
    hidden_risk: int,
) -> EngagementSignals:
    month_index = int((month.year - activation_month.year) * 12 + (month.month - activation_month.month))
    seasonality = 4.0 * np.sin((month.month / 12.0) * 2 * np.pi)
    usage = 35 + 32 * quality + 21 * growth + 5 * np.log1p(month_index + 1) + seasonality
    usage += float(rng.normal(0, 7.5))

    if hidden_risk and month_index > 10:
        usage -= 1.1 * (month_index - 10)
    if state.fragile_expansion_month is not None:
        since_fragile = int(
            (month.year - state.fragile_expansion_month.year) * 12 + (month.month - state.fragile_expansion_month.month)
        )
        if since_fragile >= 2:
            usage -= 2.3 * since_fragile

    usage = float(np.clip(usage, 5, 100))
    seats_active = int(np.clip(np.round(state.seats * (0.56 + usage / 125 + rng.normal(0, 0.05))), 1, state.seats))
    support_lambda = max(0.35, 0.35 + seats_active / 55 + (72 - usage) / 24 + 1.7 * (1 - quality))
    support_tickets = int(min(35, rng.poisson(support_lambda)))

    delay_mean = 3.5 + 11 * (1 - quality) + 9 * max(0.0, state.current_discount - 0.18)
    delay_mean += 0.8 * max(0, support_tickets - 5)
    delay_mean += {"SMB": 5.0, "Mid-Market": 2.5, "Enterprise": 1.0}[segment]
    if hidden_risk and month_index > 12:
        delay_mean += 4.5
    payment_delay = int(np.clip(np.round(rng.normal(delay_mean, 5.8)), 0, 95))

    nps = 1.9 * (usage - 50) - 1.8 * max(0, support_tickets - 6) - 0.45 * payment_delay
    nps += float(rng.normal(0, 11.5))
    nps = float(np.clip(nps, -100, 100))
    healthy_signal = (usage / 100) + max(0.0, nps) / 120 - (payment_delay / 95) - support_tickets / 40
    return EngagementSignals(
        month_index=month_index,
        usage=usage,
        seats_active=seats_active,
        support_tickets=support_tickets,
        payment_delay=payment_delay,
        nps=nps,
        healthy_signal=healthy_signal,
    )


def _tier_rank(tier: str) -> int:
    return {"Basic": 0, "Growth": 1, "Pro": 2, "Enterprise": 3}[tier]


def _plan_id_for_tier_cycle(plans: pd.DataFrame, target_tier: str, cycle: str) -> str:
    row = plans[(plans["plan_tier"] == target_tier) & (plans["billing_cycle"] == cycle)].iloc[0]
    return str(row["plan_id"])


def simulate_revenue_movement(
    rng: np.random.Generator,
    month: pd.Timestamp,
    state: CommercialState,
    plans: pd.DataFrame,
    segment: str,
    fragile: int,
    signals: EngagementSignals,
    renewal_due_flag: int,
) -> RevenueMovement:
    base_expand_prob = {"SMB": 0.014, "Mid-Market": 0.024, "Enterprise": 0.031}[segment]
    expansion_prob = base_expand_prob
    if signals.usage > 70 and signals.nps > 20 and signals.payment_delay < 14:
        expansion_prob += 0.03
    if renewal_due_flag:
        expansion_prob += 0.018
    if signals.month_index < 3:
        expansion_prob *= 0.6

    base_contr_prob = {"SMB": 0.011, "Mid-Market": 0.009, "Enterprise": 0.007}[segment]
    contraction_prob = base_contr_prob
    if signals.usage < 48:
        contraction_prob += 0.026
    if signals.payment_delay > 20:
        contraction_prob += 0.02
    if signals.support_tickets > 8:
        contraction_prob += 0.011
    if renewal_due_flag:
        contraction_prob += 0.012

    force_fragile_expansion = (
        bool(fragile)
        and state.fragile_expansion_month is None
        and 4 <= signals.month_index <= 16
        and rng.random() < 0.065
    )
    did_expand = force_fragile_expansion or (rng.random() < np.clip(expansion_prob, 0, 0.55))
    did_contract = (rng.random() < np.clip(contraction_prob, 0, 0.55)) and not did_expand
    expansion_mrr = 0.0
    contraction_mrr = 0.0
    downgrade_flag = 0

    if did_expand:
        if force_fragile_expansion:
            expansion_pct = float(rng.uniform(0.16, 0.4))
            state.current_discount = float(max(state.current_discount, rng.uniform(0.28, 0.47)))
            state.fragile_expansion_month = month
        else:
            expansion_pct = float(rng.uniform(0.05, 0.24))
            state.current_discount = float(np.clip(state.current_discount + rng.normal(-0.005, 0.012), 0.0, 0.5))

        expansion_mrr = state.current_contracted * expansion_pct
        state.current_contracted += expansion_mrr
        added_seats = int(max(1, np.round(state.seats * expansion_pct * rng.uniform(0.4, 0.85))))
        state.seats += added_seats
        if expansion_pct > 0.22 and _tier_rank(state.current_tier) < 3 and rng.random() < 0.34:
            new_tier = ["Basic", "Growth", "Pro", "Enterprise"][_tier_rank(state.current_tier) + 1]
            state.plan_id = _plan_id_for_tier_cycle(plans, new_tier, state.billing_cycle)
            state.current_tier = new_tier
    elif did_contract:
        contraction_pct = float(rng.uniform(0.06, 0.28))
        contraction_mrr = state.current_contracted * contraction_pct
        state.current_contracted = max(85.0, state.current_contracted - contraction_mrr)
        state.seats = max(2, int(np.round(state.seats * (1 - contraction_pct * rng.uniform(0.55, 0.95)))))
        downgrade_flag = 1
        if contraction_pct > 0.2 and _tier_rank(state.current_tier) > 0 and rng.random() < 0.37:
            new_tier = ["Basic", "Growth", "Pro", "Enterprise"][_tier_rank(state.current_tier) - 1]
            state.plan_id = _plan_id_for_tier_cycle(plans, new_tier, state.billing_cycle)
            state.current_tier = new_tier

    if renewal_due_flag and not did_expand:
        if signals.healthy_signal > 0.55:
            state.current_discount = float(np.clip(state.current_discount - rng.uniform(0.0, 0.02), 0.0, 0.5))
        elif signals.healthy_signal < 0.1:
            state.current_discount = float(np.clip(state.current_discount + rng.uniform(0.0, 0.03), 0.0, 0.55))
    return RevenueMovement(expansion_mrr, contraction_mrr, downgrade_flag, did_expand)


def simulate_churn(
    rng: np.random.Generator,
    month: pd.Timestamp,
    state: CommercialState,
    segment: str,
    hidden_risk: int,
    signals: EngagementSignals,
    renewal_due_flag: int,
) -> int:
    churn_prob = {"SMB": 0.0105, "Mid-Market": 0.0068, "Enterprise": 0.0038}[segment]
    if signals.usage < 35:
        churn_prob += 0.026
    elif signals.usage < 48:
        churn_prob += 0.013
    if signals.nps < -15:
        churn_prob += 0.018
    elif signals.nps < 5:
        churn_prob += 0.009
    if signals.payment_delay > 45:
        churn_prob += 0.032
    elif signals.payment_delay > 20:
        churn_prob += 0.016
    if signals.support_tickets > 10:
        churn_prob += 0.012
    if state.current_discount > 0.32:
        churn_prob += 0.009
    if hidden_risk and signals.month_index > 12:
        churn_prob += 0.011
    if state.fragile_expansion_month is not None:
        since_fragile = int(
            (month.year - state.fragile_expansion_month.year) * 12 + (month.month - state.fragile_expansion_month.month)
        )
        if 3 <= since_fragile <= 9:
            churn_prob += 0.03
    if signals.usage > 75 and signals.nps > 30 and signals.payment_delay < 10:
        churn_prob -= 0.007
    if state.billing_cycle == "annual" and renewal_due_flag == 0:
        churn_prob *= 0.22
    if renewal_due_flag == 1 and month.month in (1, 7):
        churn_prob += 0.004
    return int(rng.random() < float(np.clip(churn_prob, 0.0005, 0.45)))


def build_billing(state: CommercialState, payment_delay: int) -> BillingResult:
    billed_mrr = float(state.current_contracted)
    commercial_discount_amount = float(billed_mrr * state.current_discount)
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
    effective_adjustment = float(min(billed_mrr, commercial_discount_amount + collection_loss_amount))
    return BillingResult(
        billed_mrr=billed_mrr,
        commercial_discount_amount=commercial_discount_amount,
        collection_loss_amount=collection_loss_amount,
        effective_revenue_adjustment_amount=effective_adjustment,
        realized_mrr=float(max(0.0, billed_mrr - effective_adjustment)),
        payment_status=payment_status,
    )


def lifecycle_stage(
    churned: bool,
    month_index: int,
    renewal_due_flag: int,
    signals: EngagementSignals,
) -> str:
    if churned:
        return "Churned"
    risk_signal = (
        (1 - signals.usage / 100)
        + max(0, -signals.nps) / 120
        + signals.payment_delay / 95
        + signals.support_tickets / 35
    )
    if month_index <= 3:
        return "Onboarding"
    if renewal_due_flag == 1:
        return "Renewing Soon"
    if risk_signal > 1.45:
        return "At Risk"
    return "Active"

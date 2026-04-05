from __future__ import annotations

import csv
import unittest
from collections import defaultdict
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


def parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class TestRawDataContracts(unittest.TestCase):
    def test_required_raw_files_exist(self) -> None:
        required = [
            RAW / "customers.csv",
            RAW / "plans.csv",
            RAW / "subscriptions.csv",
            RAW / "monthly_account_metrics.csv",
            RAW / "invoices.csv",
            RAW / "account_managers.csv",
        ]
        missing = [str(p) for p in required if not p.exists()]
        self.assertEqual(missing, [], f"Missing raw files: {missing}")

    def test_primary_keys_unique(self) -> None:
        customers = read_rows(RAW / "customers.csv")
        subs = read_rows(RAW / "subscriptions.csv")
        monthly = read_rows(RAW / "monthly_account_metrics.csv")
        invoices = read_rows(RAW / "invoices.csv")

        self.assertEqual(len(customers), len({r["customer_id"] for r in customers}), "Duplicate customer_id detected")
        self.assertEqual(len(subs), len({r["subscription_id"] for r in subs}), "Duplicate subscription_id detected")
        self.assertEqual(len(invoices), len({r["invoice_id"] for r in invoices}), "Duplicate invoice_id detected")

        monthly_keys = [f'{r["customer_id"]}|{r["month"][:10]}' for r in monthly]
        self.assertEqual(len(monthly_keys), len(set(monthly_keys)), "Duplicate monthly (customer_id, month) rows detected")

    def test_signup_not_after_first_subscription(self) -> None:
        customers = read_rows(RAW / "customers.csv")
        subs = read_rows(RAW / "subscriptions.csv")

        signup_map = {r["customer_id"]: parse_date(r["signup_date"]) for r in customers}
        first_sub: dict[str, date] = {}
        for r in subs:
            cid = r["customer_id"]
            dt = parse_date(r["subscription_start_date"])
            if cid not in first_sub or dt < first_sub[cid]:
                first_sub[cid] = dt

        violations = []
        for cid, first_start in first_sub.items():
            signup = signup_map[cid]
            if signup > first_start:
                violations.append((cid, signup.isoformat(), first_start.isoformat()))

        self.assertEqual(violations, [], f"signup_date after first subscription start found: sample={violations[:5]}")

    def test_invoice_discount_logic(self) -> None:
        invoices = read_rows(RAW / "invoices.csv")
        bad_rows = []
        for r in invoices:
            billed = float(r["billed_mrr"])
            realized = float(r["realized_mrr"])
            discount = float(r["discount_amount"])

            collection_loss = float(r["collection_loss_amount"]) if "collection_loss_amount" in r else 0.0
            effective_adjustment = (
                float(r["effective_revenue_adjustment_amount"])
                if "effective_revenue_adjustment_amount" in r
                else discount
            )

            if discount < 0 or discount > billed + 1e-6:
                bad_rows.append((r["invoice_id"], billed, discount, realized, "commercial_discount_out_of_range"))
                continue
            if collection_loss < 0 or collection_loss > billed + 1e-6:
                bad_rows.append((r["invoice_id"], billed, collection_loss, realized, "collection_loss_out_of_range"))
                continue
            if effective_adjustment < 0 or effective_adjustment > billed + 1e-6:
                bad_rows.append((r["invoice_id"], billed, effective_adjustment, realized, "effective_adjustment_out_of_range"))
                continue
            implied_adjustment = round(discount + collection_loss, 2)
            if abs(effective_adjustment - implied_adjustment) > 0.02:
                bad_rows.append(
                    (
                        r["invoice_id"],
                        billed,
                        discount,
                        collection_loss,
                        effective_adjustment,
                        implied_adjustment,
                        "component_sum_mismatch",
                    )
                )
                continue
            implied_realized = round(billed - effective_adjustment, 2)
            if abs(realized - implied_realized) > 0.02:
                bad_rows.append(
                    (
                        r["invoice_id"],
                        billed,
                        discount,
                        collection_loss,
                        effective_adjustment,
                        realized,
                        implied_realized,
                        "invoice_arithmetic_mismatch",
                    )
                )
        self.assertEqual(bad_rows, [], f"Invoice discount logic violations: sample={bad_rows[:5]}")

    def test_status_values_are_known(self) -> None:
        subs = read_rows(RAW / "subscriptions.csv")
        invoices = read_rows(RAW / "invoices.csv")
        monthly = read_rows(RAW / "monthly_account_metrics.csv")

        sub_statuses = {r["status"] for r in subs}
        self.assertEqual(sub_statuses - {"active", "churned"}, set(), f"Unknown subscription statuses: {sub_statuses}")

        payment_status = {r["payment_status"] for r in invoices}
        allowed_payment = {"paid_on_time", "paid_late", "overdue", "defaulted"}
        self.assertEqual(payment_status - allowed_payment, set(), f"Unknown payment statuses: {payment_status}")

        binary_fields = ["active_flag", "churn_flag", "downgrade_flag", "renewal_due_flag"]
        violations: dict[str, int] = defaultdict(int)
        for row in monthly:
            for field in binary_fields:
                if row[field] not in {"0", "1"}:
                    violations[field] += 1
        self.assertEqual(dict(violations), {}, f"Non-binary flags detected: {dict(violations)}")


if __name__ == "__main__":
    unittest.main()

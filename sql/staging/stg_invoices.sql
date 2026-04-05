-- Staging model: invoices
-- Distinguishes commercial discount from collections loss.

select
    invoice_id,
    customer_id,
    cast(invoice_month as date) as month,
    cast(billed_mrr as double) as billed_mrr,
    cast(realized_mrr as double) as realized_mrr,
    cast(discount_amount as double) as commercial_discount_amount,
    cast(collection_loss_amount as double) as collection_loss_amount,
    cast(effective_revenue_adjustment_amount as double) as effective_revenue_adjustment_amount,
    lower(payment_status) as payment_status,
    cast(days_to_pay as integer) as days_to_pay
from invoices;

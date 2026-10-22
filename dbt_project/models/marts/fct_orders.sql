select
    order_id,
    customer_id,
    product_category,
    payment_method,
    status,
    order_ts,
    order_date,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    net_amount
from {{ ref('stg_orders') }}

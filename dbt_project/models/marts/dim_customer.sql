select
    customer_id,
    min(order_ts)  as first_order_ts,
    max(order_ts)  as last_order_ts,
    count(*)       as total_orders
from {{ ref('stg_orders') }}
group by customer_id

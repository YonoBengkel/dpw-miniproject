select
    product_category,
    count(*)           as total_orders,
    sum(net_amount)    as total_net_amount
from {{ ref('stg_orders') }}
group by product_category

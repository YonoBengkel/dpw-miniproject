select
    order_date,
    product_category,
    payment_method,
    count(*)                                                        as order_count,
    sum(quantity)                                                   as total_quantity,
    sum(case when status in ('paid','shipped') then net_amount
             else 0 end)                                            as net_sales
from {{ ref('fct_orders') }}
group by order_date, product_category, payment_method

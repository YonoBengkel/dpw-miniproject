with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    cast(order_id         as varchar)   as order_id,
    cast(customer_id      as varchar)   as customer_id,
    cast(order_ts         as timestamp) as order_ts,
    cast(order_date       as date)      as order_date,
    cast(product_category as varchar)   as product_category,
    cast(payment_method   as varchar)   as payment_method,
    cast(status           as varchar)   as status,
    cast(quantity         as integer)   as quantity,
    cast(unit_price       as double)    as unit_price,
    cast(discount_pct     as double)    as discount_pct,
    -- gross_amount dan net_amount sudah dihitung oleh scripts/transform.py (Polars)
    cast(gross_amount     as double)    as gross_amount,
    cast(net_amount       as double)    as net_amount
from source

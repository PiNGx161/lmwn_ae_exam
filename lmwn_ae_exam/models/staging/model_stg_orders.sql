{{
    config(
        materialized='table',
        alias='model_stg_orders'
    )
}}

select
    order_id,
    customer_id,
    restaurant_id,
    driver_id,
    order_datetime,
    pickup_datetime,
    delivery_datetime,
    order_status,
    delivery_zone,
    total_amount,
    payment_method,
    is_late_delivery,
    delivery_distance_km,
    date_trunc('month', order_datetime) as order_month,
    datediff('minute', order_datetime, pickup_datetime) as wait_time_min,
    datediff('minute', pickup_datetime, delivery_datetime) as delivery_time_min,
    datediff('minute', order_datetime, delivery_datetime) as total_time_min
from {{ source('raw', 'order_transactions') }}

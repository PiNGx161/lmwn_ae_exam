{{
    config(
        materialized='table',
        alias='model_int_order_enriched'
    )
}}

select
    o.order_id,
    o.customer_id,
    o.restaurant_id,
    o.driver_id,
    o.order_datetime,
    o.pickup_datetime,
    o.delivery_datetime,
    o.order_status,
    o.delivery_zone,
    o.total_amount,
    o.payment_method,
    o.is_late_delivery,
    o.delivery_distance_km,
    o.order_month,
    o.wait_time_min,
    o.delivery_time_min,
    o.total_time_min,
    c.customer_segment,
    c.customer_status,
    c.signup_date as customer_signup_date,
    c.gender,
    d.vehicle_type,
    d.region as driver_region,
    d.driver_rating,
    d.bonus_tier,
    r.restaurant_name,
    r.category as restaurant_category,
    r.city as restaurant_city,
    r.average_rating as restaurant_rating
from {{ ref('model_stg_orders') }} o
left join {{ ref('model_stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('model_stg_drivers') }} d on o.driver_id = d.driver_id
left join {{ ref('model_stg_restaurants') }} r on o.restaurant_id = r.restaurant_id

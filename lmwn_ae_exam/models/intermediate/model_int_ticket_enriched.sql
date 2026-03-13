{{
    config(
        materialized='table',
        alias='model_int_ticket_enriched'
    )
}}

select
    t.ticket_id,
    t.order_id,
    t.customer_id,
    t.driver_id,
    t.restaurant_id,
    t.issue_type,
    t.issue_sub_type,
    t.channel,
    t.opened_datetime,
    t.resolved_datetime,
    t.ticket_status,
    t.csat_score,
    t.compensation_amount,
    t.resolved_by_agent_id,
    t.opened_month,
    t.resolution_time_min,
    d.vehicle_type,
    d.region as driver_region,
    d.driver_rating,
    r.restaurant_name,
    r.category as restaurant_category,
    r.city as restaurant_city,
    o.order_status,
    o.total_amount as order_amount,
    o.delivery_zone
from {{ ref('model_stg_support_tickets') }} t
left join {{ ref('model_stg_drivers') }} d on t.driver_id = d.driver_id
left join {{ ref('model_stg_restaurants') }} r on t.restaurant_id = r.restaurant_id
left join {{ ref('model_stg_orders') }} o on t.order_id = o.order_id

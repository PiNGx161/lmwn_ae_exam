{{
    config(
        materialized='table',
        alias='report_driver_performance'
    )
}}

with driver_complaints as (
    select
        driver_id,
        count(*) as total_complaints,
        round(avg(csat_score), 2) as avg_csat_score
    from {{ ref('model_stg_support_tickets') }}
    where driver_id is not null
    group by driver_id
)

select
    dp.driver_id,
    dp.vehicle_type,
    dp.region,
    dp.active_status,
    dp.driver_rating,
    dp.bonus_tier,
    dp.total_orders_assigned,
    dp.completed_orders,
    dp.canceled_orders,
    dp.failed_orders,
    dp.completion_rate,
    dp.acceptance_rate,
    dp.avg_acceptance_time_min,
    dp.avg_delivery_time_min,
    dp.avg_total_time_min,
    dp.late_deliveries,
    dp.late_delivery_rate,
    dp.total_revenue_delivered,
    dp.avg_delivery_distance_km,
    coalesce(dc.total_complaints, 0) as total_complaints,
    dc.avg_csat_score,
    case when dp.completed_orders > 0
        then round(coalesce(dc.total_complaints, 0) * 1.0 / dp.completed_orders, 4)
        else 0 end as complaint_to_order_ratio
from {{ ref('model_int_driver_performance') }} dp
left join driver_complaints dc on dp.driver_id = dc.driver_id
order by dp.driver_id

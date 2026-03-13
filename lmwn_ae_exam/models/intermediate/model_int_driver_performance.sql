{{
    config(
        materialized='table',
        alias='model_int_driver_performance'
    )
}}

with order_stats as (
    select
        driver_id,
        count(*) as total_orders_assigned,
        count(case when order_status = 'completed' then 1 end) as completed_orders,
        count(case when order_status = 'canceled' then 1 end) as canceled_orders,
        count(case when order_status = 'failed' then 1 end) as failed_orders,
        avg(case when order_status = 'completed' then delivery_time_min end) as avg_delivery_time_min,
        avg(case when order_status = 'completed' then total_time_min end) as avg_total_time_min,
        sum(case when is_late_delivery then 1 else 0 end) as late_deliveries,
        sum(case when order_status = 'completed' then total_amount else 0 end) as total_revenue_delivered,
        avg(case when order_status = 'completed' then delivery_distance_km end) as avg_delivery_distance_km
    from {{ ref('model_stg_orders') }}
    group by driver_id
),

acceptance_stats as (
    select
        osl.order_id,
        o.driver_id,
        min(case when osl.status = 'created' then osl.status_datetime end) as created_at,
        min(case when osl.status = 'accepted' then osl.status_datetime end) as accepted_at
    from {{ ref('model_stg_order_status_logs') }} osl
    join {{ ref('model_stg_orders') }} o on osl.order_id = o.order_id
    group by osl.order_id, o.driver_id
),

acceptance_agg as (
    select
        driver_id,
        avg(datediff('minute', created_at, accepted_at)) as avg_acceptance_time_min,
        count(case when accepted_at is not null then 1 end) as accepted_count,
        count(*) as total_assigned
    from acceptance_stats
    group by driver_id
)

select
    d.driver_id,
    d.vehicle_type,
    d.region,
    d.active_status,
    d.driver_rating,
    d.bonus_tier,
    os.total_orders_assigned,
    os.completed_orders,
    os.canceled_orders,
    os.failed_orders,
    round(os.avg_delivery_time_min, 2) as avg_delivery_time_min,
    round(os.avg_total_time_min, 2) as avg_total_time_min,
    os.late_deliveries,
    case when os.completed_orders > 0 then round(os.late_deliveries * 1.0 / os.completed_orders, 4) else 0 end as late_delivery_rate,
    case when os.total_orders_assigned > 0 then round(os.completed_orders * 1.0 / os.total_orders_assigned, 4) else 0 end as completion_rate,
    os.total_revenue_delivered,
    round(os.avg_delivery_distance_km, 2) as avg_delivery_distance_km,
    round(aa.avg_acceptance_time_min, 2) as avg_acceptance_time_min,
    case when aa.total_assigned > 0 then round(aa.accepted_count * 1.0 / aa.total_assigned, 4) else 0 end as acceptance_rate
from {{ ref('model_stg_drivers') }} d
left join order_stats os on d.driver_id = os.driver_id
left join acceptance_agg aa on d.driver_id = aa.driver_id

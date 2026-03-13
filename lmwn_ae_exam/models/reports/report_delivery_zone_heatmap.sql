{{
    config(
        materialized='table',
        alias='report_delivery_zone_heatmap'
    )
}}

with zone_orders as (
    select
        delivery_zone,
        order_month,
        count(*) as total_orders,
        count(case when order_status = 'completed' then 1 end) as completed_orders,
        count(case when order_status = 'canceled' then 1 end) as canceled_orders,
        count(case when order_status = 'failed' then 1 end) as failed_orders,
        round(avg(case when order_status = 'completed' then delivery_time_min end), 2) as avg_delivery_time_min,
        round(avg(case when order_status = 'completed' then total_time_min end), 2) as avg_total_time_min,
        sum(case when is_late_delivery then 1 else 0 end) as late_deliveries,
        count(distinct driver_id) as active_drivers,
        round(avg(case when order_status = 'completed' then delivery_distance_km end), 2) as avg_delivery_distance_km,
        round(sum(case when order_status = 'completed' then total_amount else 0 end), 2) as total_revenue
    from {{ ref('model_stg_orders') }}
    group by delivery_zone, order_month
),

zone_drivers as (
    select
        region as delivery_zone,
        count(*) as total_registered_drivers
    from {{ ref('model_stg_drivers') }}
    group by region
)

select
    zo.delivery_zone,
    zo.order_month,
    zo.total_orders,
    zo.completed_orders,
    zo.canceled_orders,
    zo.failed_orders,
    case when zo.total_orders > 0
        then round(zo.completed_orders * 1.0 / zo.total_orders, 4)
        else 0 end as completion_rate,
    case when zo.total_orders > 0
        then round(zo.canceled_orders * 1.0 / zo.total_orders, 4)
        else 0 end as cancellation_rate,
    zo.avg_delivery_time_min,
    zo.avg_total_time_min,
    zo.late_deliveries,
    case when zo.completed_orders > 0
        then round(zo.late_deliveries * 1.0 / zo.completed_orders, 4)
        else 0 end as late_delivery_rate,
    zo.active_drivers,
    coalesce(zd.total_registered_drivers, 0) as total_registered_drivers,
    case when zo.active_drivers > 0
        then round(zo.total_orders * 1.0 / zo.active_drivers, 2)
        else null end as orders_per_driver,
    zo.avg_delivery_distance_km,
    zo.total_revenue
from zone_orders zo
left join zone_drivers zd on zo.delivery_zone = zd.delivery_zone
order by zo.delivery_zone, zo.order_month

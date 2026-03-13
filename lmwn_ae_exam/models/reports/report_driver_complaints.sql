{{
    config(
        materialized='table',
        alias='report_driver_complaints'
    )
}}

with driver_complaint_detail as (
    select
        t.driver_id,
        d.vehicle_type,
        d.region,
        d.driver_rating,
        d.active_status,
        count(*) as total_complaints,
        count(case when t.issue_type = 'rider' then 1 end) as rider_complaints,
        count(case when t.issue_type = 'delivery' then 1 end) as delivery_complaints,
        count(case when t.issue_sub_type = 'rude' then 1 end) as rude_complaints,
        count(case when t.issue_sub_type = 'no_mask' then 1 end) as no_mask_complaints,
        count(case when t.issue_sub_type = 'late' then 1 end) as late_complaints,
        count(case when t.issue_sub_type = 'not_delivered' then 1 end) as not_delivered_complaints,
        round(avg(t.resolution_time_min), 2) as avg_resolution_time_min,
        round(avg(t.csat_score), 2) as avg_csat_after_complaint,
        sum(t.compensation_amount) as total_compensation
    from {{ ref('model_int_ticket_enriched') }} t
    left join {{ ref('model_stg_drivers') }} d on t.driver_id = d.driver_id
    where t.driver_id is not null
    group by t.driver_id, d.vehicle_type, d.region, d.driver_rating, d.active_status
),

driver_order_totals as (
    select
        driver_id,
        count(*) as total_orders
    from {{ ref('model_stg_orders') }}
    group by driver_id
)

select
    dcd.driver_id,
    dcd.vehicle_type,
    dcd.region,
    dcd.driver_rating,
    dcd.active_status,
    dcd.total_complaints,
    dcd.rider_complaints,
    dcd.delivery_complaints,
    dcd.rude_complaints,
    dcd.no_mask_complaints,
    dcd.late_complaints,
    dcd.not_delivered_complaints,
    dcd.avg_resolution_time_min,
    dcd.avg_csat_after_complaint,
    dcd.total_compensation,
    dot.total_orders,
    case when dot.total_orders > 0
        then round(dcd.total_complaints * 1.0 / dot.total_orders, 4)
        else 0 end as complaint_to_order_ratio
from driver_complaint_detail dcd
left join driver_order_totals dot on dcd.driver_id = dot.driver_id
order by dcd.total_complaints desc

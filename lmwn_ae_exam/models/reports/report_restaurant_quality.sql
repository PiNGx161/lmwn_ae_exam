{{
    config(
        materialized='table',
        alias='report_restaurant_quality'
    )
}}

with restaurant_complaint_detail as (
    select
        t.restaurant_id,
        r.restaurant_name,
        r.category as restaurant_category,
        r.city,
        r.average_rating,
        count(*) as total_complaints,
        count(case when t.issue_type = 'food' then 1 end) as food_complaints,
        count(case when t.issue_sub_type = 'wrong_item' then 1 end) as wrong_item_complaints,
        count(case when t.issue_sub_type = 'cold' then 1 end) as cold_food_complaints,
        count(case when t.issue_type = 'payment' then 1 end) as payment_complaints,
        round(avg(t.resolution_time_min), 2) as avg_resolution_time_min,
        round(avg(t.csat_score), 2) as avg_csat_score,
        sum(t.compensation_amount) as total_compensation,
        round(avg(t.compensation_amount), 2) as avg_compensation,
        count(distinct t.customer_id) as unique_affected_customers
    from {{ ref('model_int_ticket_enriched') }} t
    left join {{ ref('model_stg_restaurants') }} r on t.restaurant_id = r.restaurant_id
    where t.restaurant_id is not null
    group by t.restaurant_id, r.restaurant_name, r.category, r.city, r.average_rating
),

restaurant_order_totals as (
    select
        restaurant_id,
        count(*) as total_orders,
        count(distinct customer_id) as total_unique_customers
    from {{ ref('model_stg_orders') }}
    group by restaurant_id
),

repeat_impact as (
    select
        t.restaurant_id,
        count(distinct t.customer_id) as customers_with_complaints,
        count(distinct case when exists (
            select 1 from {{ ref('model_stg_orders') }} o2
            where o2.customer_id = t.customer_id
              and o2.restaurant_id = t.restaurant_id
              and o2.order_datetime > t.opened_datetime
        ) then t.customer_id end) as customers_reordered_after_complaint
    from {{ ref('model_int_ticket_enriched') }} t
    where t.restaurant_id is not null
    group by t.restaurant_id
)

select
    rcd.restaurant_id,
    rcd.restaurant_name,
    rcd.restaurant_category,
    rcd.city,
    rcd.average_rating,
    rcd.total_complaints,
    rcd.food_complaints,
    rcd.wrong_item_complaints,
    rcd.cold_food_complaints,
    rcd.payment_complaints,
    rcd.avg_resolution_time_min,
    rcd.avg_csat_score,
    rcd.total_compensation,
    rcd.avg_compensation,
    rcd.unique_affected_customers,
    rot.total_orders,
    case when rot.total_orders > 0
        then round(rcd.total_complaints * 1.0 / rot.total_orders, 4)
        else 0 end as complaint_to_order_ratio,
    ri.customers_with_complaints,
    ri.customers_reordered_after_complaint,
    case when ri.customers_with_complaints > 0
        then round(ri.customers_reordered_after_complaint * 1.0 / ri.customers_with_complaints, 4)
        else 0 end as repeat_after_complaint_rate
from restaurant_complaint_detail rcd
left join restaurant_order_totals rot on rcd.restaurant_id = rot.restaurant_id
left join repeat_impact ri on rcd.restaurant_id = ri.restaurant_id
order by rcd.total_complaints desc

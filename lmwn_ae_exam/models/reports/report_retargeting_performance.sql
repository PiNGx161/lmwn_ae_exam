{{
    config(
        materialized='table',
        alias='report_retargeting_performance'
    )
}}

with retargeting_campaigns as (
    select *
    from {{ ref('model_stg_campaigns') }}
    where campaign_type = 'retargeting'
),

retarget_interactions as (
    select
        ci.campaign_id,
        ci.customer_id,
        ci.is_new_customer,
        min(ci.interaction_datetime) as first_retarget_datetime,
        max(case when ci.event_type = 'conversion' then ci.interaction_datetime end) as conversion_datetime,
        count(case when ci.event_type = 'conversion' then 1 end) as conversion_count,
        sum(case when ci.event_type = 'conversion' then ci.revenue else 0 end) as retarget_revenue,
        sum(ci.ad_cost) as retarget_cost
    from {{ ref('model_stg_campaign_interactions') }} ci
    inner join retargeting_campaigns rc on ci.campaign_id = rc.campaign_id
    group by ci.campaign_id, ci.customer_id, ci.is_new_customer
),

customer_order_history as (
    select
        customer_id,
        min(order_datetime) as first_ever_order,
        max(order_datetime) as last_order_before_retarget,
        count(*) as total_historical_orders
    from {{ ref('model_stg_orders') }}
    group by customer_id
),

retarget_analysis as (
    select
        ri.campaign_id,
        ri.customer_id,
        ri.is_new_customer,
        ri.first_retarget_datetime,
        ri.conversion_datetime,
        ri.conversion_count,
        ri.retarget_revenue,
        ri.retarget_cost,
        coh.first_ever_order,
        coh.last_order_before_retarget,
        coh.total_historical_orders,
        datediff('day', coh.last_order_before_retarget, ri.first_retarget_datetime) as days_since_last_order,
        case when ri.conversion_count > 0 then true else false end as did_return
    from retarget_interactions ri
    left join customer_order_history coh on ri.customer_id = coh.customer_id
)

select
    ra.campaign_id,
    rc.campaign_name,
    rc.channel,
    rc.targeting_strategy,
    rc.objective,
    count(distinct ra.customer_id) as targeted_customers,
    count(distinct case when ra.did_return then ra.customer_id end) as returned_customers,
    round(count(distinct case when ra.did_return then ra.customer_id end) * 1.0 /
        nullif(count(distinct ra.customer_id), 0), 4) as return_rate,
    round(sum(ra.retarget_revenue), 2) as total_spend_by_retargeted,
    round(avg(ra.retarget_revenue), 2) as avg_spend_per_customer,
    round(avg(ra.days_since_last_order), 2) as avg_days_since_last_order,
    round(sum(ra.retarget_cost), 2) as total_retarget_cost,
    case when sum(ra.retarget_cost) > 0
        then round(sum(ra.retarget_revenue) / sum(ra.retarget_cost), 4)
        else null end as retarget_roas,
    count(distinct case when not ra.is_new_customer then ra.customer_id end) as existing_customers_targeted,
    round(avg(ra.total_historical_orders), 2) as avg_historical_orders
from retarget_analysis ra
left join retargeting_campaigns rc on ra.campaign_id = rc.campaign_id
group by ra.campaign_id, rc.campaign_name, rc.channel, rc.targeting_strategy, rc.objective
order by ra.campaign_id

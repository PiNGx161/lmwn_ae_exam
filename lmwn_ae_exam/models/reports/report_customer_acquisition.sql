{{
    config(
        materialized='table',
        alias='report_customer_acquisition'
    )
}}

with new_customer_interactions as (
    select
        ci.campaign_id,
        ci.customer_id,
        ci.platform,
        min(ci.interaction_datetime) as first_interaction_datetime,
        min(case when ci.event_type = 'conversion' then ci.interaction_datetime end) as first_conversion_datetime
    from {{ ref('model_stg_campaign_interactions') }} ci
    where ci.is_new_customer = true
    group by ci.campaign_id, ci.customer_id, ci.platform
),

new_customer_orders as (
    select
        nci.campaign_id,
        nci.customer_id,
        nci.platform,
        nci.first_interaction_datetime,
        nci.first_conversion_datetime,
        co.total_orders,
        co.completed_orders,
        co.total_spend,
        co.avg_order_value,
        co.first_order_datetime,
        co.last_order_datetime,
        co.active_days_span,
        co.is_repeat_customer,
        datediff('hour', nci.first_interaction_datetime, co.first_order_datetime) as hours_to_first_purchase
    from new_customer_interactions nci
    left join {{ ref('model_int_customer_orders') }} co on nci.customer_id = co.customer_id
),

campaign_costs as (
    select
        campaign_id,
        sum(ad_cost) as total_campaign_cost
    from {{ ref('model_stg_campaign_interactions') }}
    group by campaign_id
)

select
    nco.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.channel,
    nco.platform,
    count(distinct nco.customer_id) as new_customers_acquired,
    round(avg(nco.total_spend), 2) as avg_customer_total_spend,
    round(avg(nco.avg_order_value), 2) as avg_order_value,
    round(avg(nco.completed_orders), 2) as avg_orders_per_customer,
    sum(case when nco.is_repeat_customer then 1 else 0 end) as repeat_customers,
    round(avg(case when nco.is_repeat_customer then 1.0 else 0 end), 4) as repeat_rate,
    round(avg(nco.active_days_span), 2) as avg_active_days,
    round(avg(nco.hours_to_first_purchase), 2) as avg_hours_to_first_purchase,
    round(cc.total_campaign_cost, 2) as total_marketing_cost,
    case when count(distinct nco.customer_id) > 0
        then round(cc.total_campaign_cost / count(distinct nco.customer_id), 2)
        else null end as cac
from new_customer_orders nco
left join {{ ref('model_stg_campaigns') }} c on nco.campaign_id = c.campaign_id
left join campaign_costs cc on nco.campaign_id = cc.campaign_id
group by
    nco.campaign_id, c.campaign_name, c.campaign_type, c.channel,
    nco.platform, cc.total_campaign_cost
order by nco.campaign_id, nco.platform

{{
    config(
        materialized='table',
        alias='model_int_campaign_performance'
    )
}}

with interactions_agg as (
    select
        ci.campaign_id,
        date_trunc('month', ci.interaction_datetime) as report_month,
        count(*) as total_interactions,
        count(case when ci.event_type = 'impression' then 1 end) as impressions,
        count(case when ci.event_type = 'click' then 1 end) as clicks,
        count(case when ci.event_type = 'conversion' then 1 end) as conversions,
        sum(ci.ad_cost) as total_ad_cost,
        sum(case when ci.event_type = 'conversion' then ci.revenue else 0 end) as total_revenue,
        count(distinct ci.customer_id) as unique_customers,
        count(distinct case when ci.is_new_customer then ci.customer_id end) as new_customers,
        count(distinct case when ci.event_type = 'conversion' then ci.customer_id end) as converting_customers
    from {{ ref('model_stg_campaign_interactions') }} ci
    group by ci.campaign_id, date_trunc('month', ci.interaction_datetime)
)

select
    ia.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.objective,
    c.channel,
    c.budget,
    c.cost_model,
    c.targeting_strategy,
    ia.report_month,
    ia.total_interactions,
    ia.impressions,
    ia.clicks,
    ia.conversions,
    ia.total_ad_cost,
    ia.total_revenue,
    ia.unique_customers,
    ia.new_customers,
    ia.converting_customers,
    case when ia.impressions > 0 then round(ia.clicks * 1.0 / ia.impressions, 4) else 0 end as ctr,
    case when ia.clicks > 0 then round(ia.conversions * 1.0 / ia.clicks, 4) else 0 end as cvr,
    case when ia.converting_customers > 0 then round(ia.total_ad_cost / ia.converting_customers, 2) else null end as cpa,
    case when ia.total_ad_cost > 0 then round(ia.total_revenue / ia.total_ad_cost, 4) else null end as roas
from interactions_agg ia
left join {{ ref('model_stg_campaigns') }} c on ia.campaign_id = c.campaign_id

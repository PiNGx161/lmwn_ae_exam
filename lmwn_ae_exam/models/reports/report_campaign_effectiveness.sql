{{
    config(
        materialized='table',
        alias='report_campaign_effectiveness'
    )
}}

select
    campaign_id,
    campaign_name,
    campaign_type,
    objective,
    channel,
    budget,
    cost_model,
    targeting_strategy,
    report_month,
    impressions,
    clicks,
    conversions,
    round(total_ad_cost, 2),
    round(total_revenue, 2),
    unique_customers,
    new_customers,
    converting_customers,
    ctr,
    cvr,
    cpa,
    roas,
    case when impressions > 0 then round(total_ad_cost / impressions * 1000, 2) else null end as cpm,
    case when clicks > 0 then round(total_ad_cost / clicks, 2) else null end as cpc
from {{ ref('model_int_campaign_performance') }}
order by campaign_id, report_month

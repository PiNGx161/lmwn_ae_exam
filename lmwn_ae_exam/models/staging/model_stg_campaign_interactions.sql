{{
    config(
        materialized='table',
        alias='model_stg_campaign_interactions'
    )
}}

select
    interaction_id,
    campaign_id,
    customer_id,
    interaction_datetime,
    event_type,
    platform,
    device_type,
    ad_cost,
    order_id,
    is_new_customer,
    revenue,
    session_id,
    date_trunc('month', interaction_datetime) as interaction_month
from {{ source('raw', 'campaign_interactions') }}

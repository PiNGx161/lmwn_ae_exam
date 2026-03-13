{{
    config(
        materialized='table',
        alias='model_stg_campaigns'
    )
}}

select
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    campaign_type,
    objective,
    channel,
    budget,
    cost_model,
    targeting_strategy,
    is_active
from {{ source('raw', 'campaign_master') }}

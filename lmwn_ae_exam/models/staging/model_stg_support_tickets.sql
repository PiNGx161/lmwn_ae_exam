{{
    config(
        materialized='table',
        alias='model_stg_support_tickets'
    )
}}

select
    ticket_id,
    order_id,
    customer_id,
    driver_id,
    restaurant_id,
    issue_type,
    issue_sub_type,
    channel,
    opened_datetime,
    resolved_datetime,
    status as ticket_status,
    csat_score,
    compensation_amount,
    resolved_by_agent_id,
    date_trunc('month', opened_datetime) as opened_month,
    datediff('minute', opened_datetime, resolved_datetime) as resolution_time_min
from {{ source('raw', 'support_tickets') }}

{{
    config(
        materialized='table',
        alias='model_stg_support_ticket_logs'
    )
}}

select
    log_id,
    ticket_id,
    status,
    status_datetime,
    agent_id
from {{ source('raw', 'support_ticket_status_logs') }}

{{
    config(
        materialized='table',
        alias='model_stg_order_status_logs'
    )
}}

select
    log_id,
    order_id,
    status,
    status_datetime,
    updated_by
from {{ source('raw', 'order_log_incentive_sessions_order_status_logs') }}

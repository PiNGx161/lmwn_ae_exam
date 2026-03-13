{{
    config(
        materialized='table',
        alias='model_stg_app_sessions'
    )
}}

select
    session_id,
    customer_id,
    session_start,
    session_end,
    device_type,
    os_version,
    app_version,
    location,
    datediff('minute', session_start, session_end) as session_duration_min
from {{ source('raw', 'order_log_incentive_sessions_customer_app_sessions') }}

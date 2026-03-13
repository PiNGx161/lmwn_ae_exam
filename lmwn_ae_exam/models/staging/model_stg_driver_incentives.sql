{{
    config(
        materialized='table',
        alias='model_stg_driver_incentives'
    )
}}

select
    log_id,
    driver_id,
    incentive_program,
    bonus_amount,
    applied_date,
    delivery_target,
    actual_deliveries,
    bonus_qualified,
    region
from {{ source('raw', 'order_log_incentive_sessions_driver_incentive_logs') }}

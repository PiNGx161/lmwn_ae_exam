{{
    config(
        materialized='table',
        alias='model_stg_drivers'
    )
}}

select
    driver_id,
    join_date,
    vehicle_type,
    region,
    active_status,
    driver_rating,
    bonus_tier
from {{ source('raw', 'drivers_master') }}

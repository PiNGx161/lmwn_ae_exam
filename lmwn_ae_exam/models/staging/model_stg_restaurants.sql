{{
    config(
        materialized='table',
        alias='model_stg_restaurants'
    )
}}

select
    restaurant_id,
    name as restaurant_name,
    category,
    city,
    average_rating,
    active_status,
    prep_time_min
from {{ source('raw', 'restaurants_master') }}

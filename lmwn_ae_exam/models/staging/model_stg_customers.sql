{{
    config(
        materialized='table',
        alias='model_stg_customers'
    )
}}

select
    customer_id,
    signup_date,
    customer_segment,
    status as customer_status,
    referral_source,
    birth_year,
    gender,
    preferred_device
from {{ source('raw', 'customers_master') }}

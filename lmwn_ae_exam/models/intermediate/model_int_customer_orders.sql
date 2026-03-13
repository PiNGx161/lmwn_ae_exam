{{
    config(
        materialized='table',
        alias='model_int_customer_orders'
    )
}}

with order_summary as (
    select
        customer_id,
        count(*) as total_orders,
        count(case when order_status = 'completed' then 1 end) as completed_orders,
        sum(case when order_status = 'completed' then total_amount else 0 end) as total_spend,
        avg(case when order_status = 'completed' then total_amount end) as avg_order_value,
        min(order_datetime) as first_order_datetime,
        max(order_datetime) as last_order_datetime,
        datediff('day', min(order_datetime), max(order_datetime)) as active_days_span
    from {{ ref('model_stg_orders') }}
    group by customer_id
)

select
    os.customer_id,
    c.signup_date,
    c.customer_segment,
    c.customer_status,
    c.referral_source,
    c.gender,
    os.total_orders,
    os.completed_orders,
    os.total_spend,
    round(os.avg_order_value, 2) as avg_order_value,
    os.first_order_datetime,
    os.last_order_datetime,
    os.active_days_span,
    case when os.completed_orders > 1 then true else false end as is_repeat_customer
from order_summary os
left join {{ ref('model_stg_customers') }} c on os.customer_id = c.customer_id

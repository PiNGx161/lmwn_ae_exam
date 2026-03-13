{{
    config(
        materialized='table',
        alias='report_complaint_summary'
    )
}}

select
    opened_month as report_month,
    issue_type,
    issue_sub_type,
    count(*) as total_tickets,
    count(case when ticket_status = 'resolved' then 1 end) as resolved_tickets,
    count(case when ticket_status != 'resolved' then 1 end) as unresolved_tickets,
    round(avg(resolution_time_min), 2) as avg_resolution_time_min,
    round(min(resolution_time_min), 2) as min_resolution_time_min,
    round(max(resolution_time_min), 2) as max_resolution_time_min,
    sum(compensation_amount) as total_compensation,
    round(avg(compensation_amount), 2) as avg_compensation,
    round(avg(csat_score), 2) as avg_csat_score,
    count(distinct customer_id) as unique_customers,
    count(distinct driver_id) as unique_drivers_involved,
    count(distinct restaurant_id) as unique_restaurants_involved
from {{ ref('model_int_ticket_enriched') }}
group by opened_month, issue_type, issue_sub_type
order by opened_month, issue_type, issue_sub_type

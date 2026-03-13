{{
    config(
        materialized='table',
        alias='report_driver_incentive_impact'
    )
}}

with incentive_summary as (
    select
        di.incentive_program,
        di.region,
        count(distinct di.driver_id) as participating_drivers,
        count(*) as total_incentive_records,
        sum(case when di.bonus_qualified then 1 else 0 end) as qualified_count,
        round(avg(case when di.bonus_qualified then 1.0 else 0 end), 4) as qualification_rate,
        round(sum(case when di.bonus_qualified then di.bonus_amount else 0 end), 2) as total_bonus_paid,
        round(avg(di.bonus_amount), 2) as avg_bonus_amount,
        round(avg(di.delivery_target), 2) as avg_delivery_target,
        round(avg(di.actual_deliveries), 2) as avg_actual_deliveries,
        round(avg(di.actual_deliveries * 1.0 / nullif(di.delivery_target, 0)), 4) as avg_target_achievement_rate
    from {{ ref('model_stg_driver_incentives') }} di
    group by di.incentive_program, di.region
),

driver_perf_during_incentive as (
    select
        di.incentive_program,
        di.region,
        round(avg(dp.avg_delivery_time_min), 2) as avg_driver_delivery_time,
        round(avg(dp.completion_rate), 4) as avg_driver_completion_rate,
        round(avg(dp.acceptance_rate), 4) as avg_driver_acceptance_rate,
        round(avg(dp.late_delivery_rate), 4) as avg_driver_late_rate,
        round(sum(dp.total_revenue_delivered), 2) as total_revenue_by_participants
    from {{ ref('model_stg_driver_incentives') }} di
    left join {{ ref('model_int_driver_performance') }} dp on di.driver_id = dp.driver_id
    group by di.incentive_program, di.region
)

select
    isa.incentive_program,
    isa.region,
    isa.participating_drivers,
    isa.total_incentive_records,
    isa.qualified_count,
    isa.qualification_rate,
    isa.total_bonus_paid,
    isa.avg_bonus_amount,
    isa.avg_delivery_target,
    isa.avg_actual_deliveries,
    isa.avg_target_achievement_rate,
    dpi.avg_driver_delivery_time,
    dpi.avg_driver_completion_rate,
    dpi.avg_driver_acceptance_rate,
    dpi.avg_driver_late_rate,
    dpi.total_revenue_by_participants,
    case when isa.total_bonus_paid > 0
        then round(dpi.total_revenue_by_participants / isa.total_bonus_paid, 4)
        else null end as revenue_to_bonus_ratio
from incentive_summary isa
left join driver_perf_during_incentive dpi
    on isa.incentive_program = dpi.incentive_program
    and isa.region = dpi.region
order by isa.incentive_program, isa.region

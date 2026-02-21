{{ config(materialized='view') }}

select
    fs.partition_dt,
    db.branch_name,
    db.parish,
    db.county,
    count(*)                                          as total_sessions,
    sum(fs.minutes_read)                              as total_minutes_read,
    round(avg(fs.minutes_read), 1)                    as avg_minutes_per_session,
    count(distinct fs.child_id)                       as unique_readers,
    sum(case when fs.is_auto_checkout then 1 else 0 end) as auto_checkout_sessions
from {{ ref('fact_sessions') }} fs
left join {{ ref('dim_branches') }} db on fs.branch_id = db.branch_id
group by 1, 2, 3, 4
order by 1 desc, 5 desc

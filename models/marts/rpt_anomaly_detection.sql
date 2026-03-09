{{ config(materialized='view') }}

/*
  rpt_anomaly_detection
  ─────────────────────
  Flags three categories of unusual activity in the session pipeline.
  Runs daily as part of the dbt cron job. Intended to be reviewed by
  programme coordinators before the monthly Step Functions approval gate.

  Signal 1 — SPIKE
    A branch logs ≥3× its own 28-day rolling average on a given day,
    with a minimum of 5 sessions (avoids noise at low-volume branches).

  Signal 2 — SILENCE
    A branch that was active in the past 28 days logs zero sessions
    on a given partition date. Indicates connectivity issues, PWA
    problems, or an unexpected branch closure.

  Signal 3 — CHILD_CONCENTRATION
    A single child accounts for ≥50% of a branch's sessions on a day
    with at least 4 total sessions. Potential scoring manipulation.

  Columns
  ───────
  partition_dt        date of the activity (or date of the silence)
  branch_id           library branch
  branch_name         human-readable name
  parish              parish
  flag_type           SPIKE | SILENCE | CHILD_CONCENTRATION
  sessions_on_day     sessions logged on partition_dt (0 for SILENCE)
  rolling_avg_28d     branch's 28-day rolling average (null for SILENCE)
  spike_ratio         sessions_on_day / rolling_avg_28d (null if avg=0)
  top_child_id        child with most sessions on that day (CONCENTRATION only)
  top_child_sessions  sessions from that child (CONCENTRATION only)
  top_child_pct       share of branch sessions from that child
  notes               human-readable description of the flag
*/

with daily_counts as (
    select
        fs.partition_dt,
        fs.branch_id,
        db.branch_name,
        db.parish,
        count(*)                    as sessions_on_day,
        count(distinct fs.child_id) as unique_children
    from {{ ref('fact_sessions') }} fs
    left join {{ ref('dim_branches') }} db
        on fs.branch_id = db.branch_id
    group by 1, 2, 3, 4
),

-- 28-day rolling average per branch, excluding the current day
-- uses ROWS BETWEEN so the average never includes today's count
rolling_baseline as (
    select
        partition_dt,
        branch_id,
        branch_name,
        parish,
        sessions_on_day,
        unique_children,
        round(
            avg(cast(sessions_on_day as double)) over (
                partition by branch_id
                order by partition_dt
                rows between 28 preceding and 1 preceding
            ), 2
        ) as rolling_avg_28d,
        count(*) over (
            partition by branch_id
            order by partition_dt
            rows between 28 preceding and 1 preceding
        ) as days_in_window
    from daily_counts
),

-- Signal 1: session spikes
spikes as (
    select
        partition_dt,
        branch_id,
        branch_name,
        parish,
        'SPIKE'                                         as flag_type,
        sessions_on_day,
        rolling_avg_28d,
        round(
            cast(sessions_on_day as double) / nullif(rolling_avg_28d, 0),
            2
        )                                               as spike_ratio,
        null                                            as top_child_id,
        null                                            as top_child_sessions,
        null                                            as top_child_pct,
        concat(
            cast(sessions_on_day as varchar),
            ' sessions vs 28-day avg of ',
            cast(rolling_avg_28d as varchar),
            ' (',
            cast(
                round(
                    cast(sessions_on_day as double) / nullif(rolling_avg_28d, 0),
                    1
                ) as varchar
            ),
            'x baseline)'
        )                                               as notes
    from rolling_baseline
    where
        sessions_on_day >= 5
        and rolling_avg_28d > 0
        and days_in_window >= 7  -- need at least 7 days of history to flag
        and cast(sessions_on_day as double) / nullif(rolling_avg_28d, 0) >= 3.0
),

-- Signal 2: silence — branches active in last 28 days with zero sessions today
-- requires a cross-join between active branches and all partition dates
all_dates as (
    select distinct partition_dt from daily_counts
),

active_branches as (
    select distinct
        branch_id,
        branch_name,
        parish
    from daily_counts
    where partition_dt >= cast(date_format(date_add('day', -28, current_date), '%Y-%m-%d') as varchar)
),

silence_candidates as (
    select
        d.partition_dt,
        b.branch_id,
        b.branch_name,
        b.parish
    from all_dates d
    cross join active_branches b
),

silence as (
    select
        sc.partition_dt,
        sc.branch_id,
        sc.branch_name,
        sc.parish,
        'SILENCE'   as flag_type,
        0           as sessions_on_day,
        null        as rolling_avg_28d,
        null        as spike_ratio,
        null        as top_child_id,
        null        as top_child_sessions,
        null        as top_child_pct,
        concat(
            'No sessions recorded at ',
            sc.branch_name,
            ' on ',
            cast(sc.partition_dt as varchar),
            ' despite activity in the past 28 days'
        )           as notes
    from silence_candidates sc
    left join daily_counts dc
        on  sc.partition_dt = dc.partition_dt
        and sc.branch_id    = dc.branch_id
    where dc.branch_id is null
      -- only flag recent silences to avoid noise from historical gaps
      and sc.partition_dt >= cast(date_format(date_add('day', -7, current_date), '%Y-%m-%d') as varchar)
),

-- Signal 3: child concentration
child_daily as (
    select
        fs.partition_dt,
        fs.branch_id,
        fs.child_id,
        count(*) as child_sessions
    from {{ ref('fact_sessions') }} fs
    group by 1, 2, 3
),

concentration as (
    select
        cd.partition_dt,
        cd.branch_id,
        dc.branch_name,
        dc.parish,
        'CHILD_CONCENTRATION'                           as flag_type,
        dc.sessions_on_day,
        null                                            as rolling_avg_28d,
        null                                            as spike_ratio,
        cd.child_id                                     as top_child_id,
        cd.child_sessions                               as top_child_sessions,
        round(
            cast(cd.child_sessions as double) / nullif(dc.sessions_on_day, 0) * 100,
            1
        )                                               as top_child_pct,
        concat(
            'Child ',
            cd.child_id,
            ' accounts for ',
            cast(cd.child_sessions as varchar),
            ' of ',
            cast(dc.sessions_on_day as varchar),
            ' sessions (',
            cast(
                round(
                    cast(cd.child_sessions as double) / nullif(dc.sessions_on_day, 0) * 100,
                    1
                ) as varchar
            ),
            '%)'
        )                                               as notes
    from child_daily cd
    join daily_counts dc
        on  cd.partition_dt = dc.partition_dt
        and cd.branch_id    = dc.branch_id
    where
        dc.sessions_on_day >= 4
        and cast(cd.child_sessions as double) / nullif(dc.sessions_on_day, 0) >= 0.5
        -- only flag the top child per branch per day
        and cd.child_sessions = (
            select max(child_sessions)
            from child_daily cd2
            where cd2.partition_dt = cd.partition_dt
              and cd2.branch_id    = cd.branch_id
        )
),

-- union all three signals
all_flags as (
    select * from spikes
    union all
    select * from silence
    union all
    select * from concentration
)

select
    partition_dt,
    branch_id,
    branch_name,
    parish,
    flag_type,
    sessions_on_day,
    rolling_avg_28d,
    spike_ratio,
    top_child_id,
    top_child_sessions,
    top_child_pct,
    notes,
    cast(date_format(now(), '%Y-%m-%d %H:%i:%s') as varchar) as flagged_at
from all_flags
order by partition_dt desc, flag_type, branch_name

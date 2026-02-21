{{ config(materialized='view') }}

with bronze_checkout as (
  select
    dt as partition_dt,
    payload.session_id     as session_id,
    payload.checked_in_at  as checked_in_at,
    payload.checked_out_at as checked_out_at
  from {{ source('bronze', 'bronze_checkouts_raw') }}
  where event_type = 'checkout'
),
silver_counts as (
  select
    partition_dt,
    count(distinct session_id) as silver_sessions
  from {{ ref('fact_sessions') }}
  group by 1
),
book_counts as (
  select
    partition_dt,
    count(*)                   as silver_session_book_rows,
    count(distinct session_id) as silver_sessions_with_books
  from {{ ref('fact_session_books') }}
  group by 1
),
bronze_flags as (
  select
    partition_dt,
    count(*)                                                          as bronze_checkout_events,
    sum(case when checked_out_at is null then 1 else 0 end)          as bronze_missing_checkout_time,
    sum(case when checked_in_at  is null then 1 else 0 end)          as bronze_missing_checkin_time,
    sum(case when checked_out_at is not null
              and checked_in_at  is not null
              and checked_out_at < checked_in_at then 1 else 0 end)  as bronze_checkout_before_checkin
  from bronze_checkout
  group by 1
)

select
  b.partition_dt,
  b.bronze_checkout_events,
  coalesce(sc.silver_sessions, 0)                                    as silver_sessions,
  (b.bronze_checkout_events - coalesce(sc.silver_sessions, 0))       as events_dropped_in_silver,
  b.bronze_missing_checkin_time,
  b.bronze_missing_checkout_time,
  b.bronze_checkout_before_checkin,
  coalesce(bc.silver_session_book_rows, 0)                           as silver_session_book_rows,
  coalesce(bc.silver_sessions_with_books, 0)                         as silver_sessions_with_books
from bronze_flags b
left join silver_counts sc on b.partition_dt = sc.partition_dt
left join book_counts bc   on b.partition_dt = bc.partition_dt
order by b.partition_dt desc

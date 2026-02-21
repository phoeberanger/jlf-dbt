{{ config(materialized='view') }}

with base as (
  select
    dt as partition_dt,
    event_type,
    envelope.event_id      as event_id,
    payload.session_id     as session_id,
    payload.checked_in_at  as checked_in_at,
    payload.checked_out_at as checked_out_at,
    payload.branch_id      as branch_id,
    payload.minutes_read   as minutes_read_raw
  from {{ source('bronze', 'bronze_checkouts_raw') }}
)

select
  partition_dt,
  count(*)                                                                 as raw_events,
  sum(case when event_type = 'checkout' then 1 else 0 end)                as raw_checkout_events,
  sum(case when event_type = 'checkin'  then 1 else 0 end)                as raw_checkin_events,
  sum(case when event_id       is null then 1 else 0 end)                 as missing_event_id,
  sum(case when session_id     is null then 1 else 0 end)                 as missing_session_id,
  sum(case when branch_id      is null then 1 else 0 end)                 as missing_branch_id,
  sum(case when checked_in_at  is null then 1 else 0 end)                 as missing_checked_in_at,
  sum(case when checked_out_at is null then 1 else 0 end)                 as missing_checked_out_at,
  sum(case when minutes_read_raw is null then 1 else 0 end)               as missing_minutes_read
from base
group by 1
order by 1 desc

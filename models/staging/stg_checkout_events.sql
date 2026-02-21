{{ config(materialized='view') }}

with src as (
    select
        envelope.event_id        as event_id,
        payload.session_id       as session_id,
        payload.child_id         as child_id,
        payload.branch_id        as branch_id,
        payload.librarian_id     as librarian_id,
        payload.checked_in_at    as checked_in_at,
        payload.checked_out_at   as checked_out_at,
        cast(payload.minutes_read as integer)        as minutes_read,
        coalesce(cast(payload.auto_checkout as boolean), false) as is_auto_checkout,
        payload.client_id        as client_id,
        envelope.source_system   as source_system,
        envelope.schema_version  as schema_version,
        payload.books            as books,
        dt                       as partition_dt
    from {{ source('bronze', 'bronze_checkouts_raw') }}
    where event_type = 'checkout'
      and payload.checked_out_at is not null
      and payload.checked_out_at >= payload.checked_in_at
)

select * from src

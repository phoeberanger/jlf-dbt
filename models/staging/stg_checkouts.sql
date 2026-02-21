with source as (
    select *
    from {{ source('bronze', 'bronze_checkouts_raw') }}
    where event_type = 'checkout'
),

typed as (
    select
        -- identity
        envelope.event_id                                  as event_id,
        payload.session_id                                 as session_id,
        payload.child_id                                   as child_id,
        payload.branch_id                                  as branch_id,
        payload.librarian_id                               as librarian_id,

        -- timestamps (Athena)
        cast(from_iso8601_timestamp(payload.checked_in_at) as timestamp)      as checked_in_at,
        cast(from_iso8601_timestamp(payload.checked_out_at) as timestamp)     as checked_out_at,

        -- metrics
        cast(payload.minutes_read as integer)               as minutes_read,
        cast(payload.auto_checkout as boolean)              as is_auto_checkout,

        -- lineage / partition
        payload.client_id                                   as client_id,
        envelope.source_system                              as source_system,
        envelope.schema_version                             as schema_version,
        dt                                                  as partition_dt
    from source
)

select *
from typed
where checked_out_at is not null
  and checked_out_at >= checked_in_at

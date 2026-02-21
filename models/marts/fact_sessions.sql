{{
  config(
    materialized='table',
    format='parquet',
    s3_data_dir='s3://jlf-athena-results-dev/dbt/marts/',
    partitioned_by=['partition_dt']
  )
}}

with base as (
    select *
    from {{ ref('stg_checkouts') }}
),
deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by session_id
                order by checked_out_at desc, event_id desc
            ) as rn
        from base
    )
    where rn = 1
)
select
    session_id,
    child_id,
    branch_id,
    librarian_id,
    checked_in_at,
    checked_out_at,
    minutes_read,
    is_auto_checkout,
    client_id,
    source_system,
    schema_version,
    partition_dt
from deduped

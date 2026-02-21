{{
  config(
    materialized='table',
    format='parquet',
    s3_data_dir='s3://jlf-athena-results-dev/dbt/marts/',
    partitioned_by=['partition_dt']
  )
}}

with sessions as (
    select *
    from {{ ref('stg_checkout_events') }}
    where books is not null
),
exploded as (
    select
        s.session_id,
        s.child_id,
        s.branch_id,
        s.checked_out_at,
        s.is_auto_checkout,
        s.partition_dt,
        to_hex(md5(to_utf8(lower(coalesce(nullif(trim(b.book_title), ''), 'unknown'))))) as book_key,
        coalesce(b.notes, b.librarian_notes) as notes
    from sessions s
    cross join unnest(s.books) as t(b)
    where nullif(trim(b.book_title), '') is not null
)
select
    session_id,
    child_id,
    branch_id,
    book_key,
    notes,
    checked_out_at,
    is_auto_checkout,
    partition_dt
from exploded

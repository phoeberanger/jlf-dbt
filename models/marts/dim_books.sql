{{
  config(
    materialized='table',
    format='parquet',
    s3_data_dir='s3://jlf-data-silver-dev/dbt/marts/'
  )
}}

with sessions as (
    select *
    from {{ ref('stg_checkout_events') }}
    where books is not null
),
exploded as (
    select
        b.book_title,
        b.book_genre,
        to_hex(md5(to_utf8(lower(coalesce(nullif(trim(b.book_title), ''), 'unknown'))))) as book_key
    from sessions s
    cross join unnest(s.books) as t(b)
    where nullif(trim(b.book_title), '') is not null
),
deduped as (
    select
        book_key,
        max(book_title) as book_title,
        max(book_genre) as book_genre
    from exploded
    group by book_key
)
select
    book_key,
    book_title,
    book_genre
from deduped

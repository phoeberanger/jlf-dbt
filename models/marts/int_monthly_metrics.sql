{{ config(materialized='view') }}

with sessions as (
    select
        s.child_id,
        coalesce(s.parish, e.parish)                            as parish,
        coalesce(s.date_of_birth, cast(e.date_of_birth as varchar)) as date_of_birth,
        date_trunc('month', s.checked_in_at)                    as score_month,
        date(s.checked_in_at)                                   as session_date,
        s.session_id,
        s.checked_out_at
    from {{ ref('stg_checkouts') }} s
    left join {{ ref('children_enrichment') }} e on s.child_id = e.child_id
    where s.checked_out_at is not null
),

books as (
    select
        fsb.session_id,
        db.book_genre
    from {{ ref('fact_session_books') }} fsb
    left join {{ ref('dim_books') }} db on fsb.book_key = db.book_key
),

session_agg as (
    select
        s.child_id,
        s.parish,
        s.date_of_birth,
        s.score_month,
        count(distinct s.session_date)               as visit_days,
        count(distinct s.session_id)                 as total_sessions,
        count(distinct b.session_id)                 as sessions_with_books,
        count(b.session_id)                          as total_books,
        count(distinct b.book_genre)                 as genre_count
    from sessions s
    left join books b on s.session_id = b.session_id
    group by 1, 2, 3, 4
)

select
    child_id,
    parish,
    date_of_birth,
    score_month,
    visit_days,
    total_sessions,
    sessions_with_books,
    total_books,
    genre_count,
    -- completeness: proportion of sessions that had at least one book logged
    case
        when total_sessions = 0 then 0.0
        else cast(sessions_with_books as double) / total_sessions
    end                                              as completeness_ratio
from session_agg

{{ config(materialized='table',
          format='parquet',
          s3_data_dir='s3://jlf-data-gold-dev/dbt/marts/',
          partitioned_by=['score_month']) }}

with metrics as (
    select * from {{ ref('int_monthly_metrics') }}
),

scored as (
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
        completeness_ratio,

        round(least(cast(visit_days as double) / 16.0, 1.0) * 40, 2)    as consistency_score,
        round(least(cast(total_books as double) / 20.0, 1.0) * 30, 2)   as volume_score,
        round(least(cast(genre_count as double) / 5.0, 1.0) * 20, 2)    as diversity_score,
        round(coalesce(completeness_ratio, 1.0) * 10, 2)                 as completeness_score

    from metrics
)

select
    child_id,
    parish,
    date_of_birth,
    visit_days,
    total_sessions,
    total_books,
    genre_count,
    completeness_ratio,
    consistency_score,
    volume_score,
    diversity_score,
    completeness_score,
    round(consistency_score + volume_score + diversity_score + completeness_score, 2) as total_score,
    score_month
from scored

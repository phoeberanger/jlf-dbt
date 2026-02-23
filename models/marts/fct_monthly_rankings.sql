{{ config(materialized='table',
          format='parquet',
          s3_data_dir='s3://jlf-data-gold-dev/dbt/marts/',
          partitioned_by=['score_month']) }}

with scores as (
    select * from {{ ref('fct_monthly_scores') }}
),

bracketed as (
    select
        s.*,
        case
            when cast(date_of_birth as date) is null then 'Unknown'
            when date_diff('year',
                cast(date_of_birth as date),
                cast(date_trunc('month', score_month) as date)) < 6 then null
            when date_diff('year',
                cast(date_of_birth as date),
                cast(date_trunc('month', score_month) as date)) <= 11 then 'Primary'
            else 'Secondary'
        end as age_bracket
    from scores s
),

prior_honorees as (
    select
        cast(child_id as varchar) as child_id,
        cast(cast(score_month as varchar) as date) as honoree_month
    from {{ ref('monthly_honorees_history') }}
),

ranked as (
    select
        b.child_id,
        b.parish,
        b.date_of_birth,
        b.age_bracket,
        b.score_month,
        b.visit_days,
        b.total_sessions,
        b.total_books,
        b.genre_count,
        b.consistency_score,
        b.volume_score,
        b.diversity_score,
        b.completeness_score,
        b.total_score,

        row_number() over (
            partition by b.parish, b.age_bracket, b.score_month
            order by b.total_score desc
        ) as parish_rank,

        case
            when sum(
                case
                    when h.honoree_month between
                        cast(date_add('month', -3, cast(b.score_month as date)) as date)
                        and cast(date_add('month', -1, cast(b.score_month as date)) as date)
                    then 1 else 0
                end
            ) over (partition by b.child_id) > 0
            then false
            else true
        end as repeat_eligible

    from bracketed b
    left join prior_honorees h on b.child_id = h.child_id
    where b.age_bracket is not null
)

select
    child_id,
    parish,
    date_of_birth,
    age_bracket,
    visit_days,
    total_sessions,
    total_books,
    genre_count,
    consistency_score,
    volume_score,
    diversity_score,
    completeness_score,
    total_score,
    parish_rank,
    repeat_eligible,
    score_month
from ranked

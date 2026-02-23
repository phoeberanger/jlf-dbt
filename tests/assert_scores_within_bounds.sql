-- Fails if any total_score is outside the valid 0-100 range.
-- Scores outside this range indicate a bug in the scoring algorithm.
SELECT child_id, total_score
FROM {{ ref('fct_monthly_scores') }}
WHERE total_score < 0 OR total_score > 100

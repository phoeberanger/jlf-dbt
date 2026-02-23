-- Fails if more than one child has parish_rank = 1
-- for the same parish + age_bracket + score_month combination.
-- This is the core integrity guarantee of the honoree selection system.
SELECT
    parish,
    age_bracket,
    score_month,
    COUNT(*) as rank1_count
FROM {{ ref('fct_monthly_rankings') }}
WHERE parish_rank = 1
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1

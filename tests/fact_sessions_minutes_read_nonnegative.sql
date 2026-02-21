select *
from {{ ref('fact_sessions') }}
where minutes_read < 0

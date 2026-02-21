select *
from {{ ref('fact_sessions') }}
where checked_out_at < checked_in_at

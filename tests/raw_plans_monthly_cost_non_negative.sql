select *
from {{ ref('raw_plans') }}
where monthly_cost < 0

SELECT *
FROM {{ ref('raw_plans') }}
WHERE monthly_cost < 0

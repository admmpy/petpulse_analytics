SELECT *
FROM {{ ref('int_subscriptions_resolved') }}
WHERE resolved_end_date IS NOT NULL
      AND resolved_end_date < resolved_start_date

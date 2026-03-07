SELECT
    customer_id,
    COUNT(*) AS open_billable_interval_count
    
FROM {{ ref('int_subscriptions_resolved') }}
WHERE resolved_end_date IS NULL
      AND is_billable = TRUE
GROUP BY 1
HAVING COUNT(*) > 1

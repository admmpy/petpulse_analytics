SELECT *
FROM {{ ref('raw_subscriptions') }}
WHERE subscription_end_date IS NOT NULL
      AND subscription_end_date < subscription_start_date

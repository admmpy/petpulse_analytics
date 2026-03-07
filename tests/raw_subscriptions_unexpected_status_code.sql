SELECT *
FROM {{ ref('raw_subscriptions') }}
WHERE status_code IS NULL
      OR LOWER(TRIM(status_code)) NOT IN ('active', 'pending', 'canceled', 'cancelled')

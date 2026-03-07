WITH ordered_intervals AS (
    SELECT
        customer_id,
        resolved_subscription_id,
        resolved_start_date,
        resolved_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                resolved_start_date,
                COALESCE(resolved_end_date, CAST('2999-12-31' AS DATE)),
                resolved_subscription_id,
                resolved_plan_id,
                resolved_status
        )                                                                                 AS interval_row_number

    FROM {{ ref('int_subscriptions_resolved') }}
)

SELECT
    left_interval.customer_id,
    left_interval.resolved_subscription_id                                               AS left_subscription_id,
    right_interval.resolved_subscription_id                                              AS right_subscription_id,
    left_interval.resolved_start_date,
    left_interval.resolved_end_date,
    right_interval.resolved_start_date                                                   AS overlapping_start_date,
    right_interval.resolved_end_date                                                     AS overlapping_end_date

FROM ordered_intervals AS left_interval
     INNER JOIN ordered_intervals AS right_interval ON left_interval.customer_id = right_interval.customer_id
                                                        AND left_interval.interval_row_number < right_interval.interval_row_number
                                                        AND left_interval.resolved_start_date <= COALESCE(right_interval.resolved_end_date, CAST('2999-12-31' AS DATE))
                                                        AND COALESCE(left_interval.resolved_end_date, CAST('2999-12-31' AS DATE)) >= right_interval.resolved_start_date

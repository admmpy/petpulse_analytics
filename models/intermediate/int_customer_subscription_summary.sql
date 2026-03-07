/*
This model creates a one-row-per-customer subscription summary.
It combines the resolved subscription timeline into current subscription
attributes and useful history flags for downstream customer reporting.
*/

{{
    config(
        materialized='view'
    )
}}

WITH latest_intervals AS (
    -- Rank the latest resolved interval per customer for status reporting.
    SELECT
        res.customer_id,
        res.resolved_status                                                               AS current_subscription_status,
        CASE
            WHEN res.resolved_end_date IS NULL THEN res.resolved_plan_id
            ELSE NULL
        END                                                                               AS current_plan_id,
        CASE
            WHEN res.resolved_end_date IS NULL THEN pln.plan_name
            ELSE NULL
        END                                                                               AS current_plan_name,
        CASE
            WHEN res.resolved_end_date IS NULL THEN res.monthly_cost
            ELSE NULL
        END                                                                               AS current_monthly_cost,
        CASE
            WHEN res.resolved_end_date IS NULL THEN res.resolved_start_date
            ELSE NULL
        END                                                                               AS current_subscription_start_date,
        ROW_NUMBER() OVER (
            PARTITION BY res.customer_id
            ORDER BY
                res.resolved_start_date DESC,
                COALESCE(res.resolved_end_date, CAST('2999-12-31' AS DATE)) DESC,
                CASE WHEN res.is_billable THEN 1 ELSE 0 END DESC,
                res.monthly_cost DESC,
                res.resolved_subscription_id ASC
        )                                                                                 AS current_interval_rank

    FROM {{ ref('int_subscriptions_resolved') }} AS res
         LEFT JOIN {{ ref('raw_plans') }}        AS pln ON res.resolved_plan_id = pln.plan_id
),

current_one_per_customer AS (
    -- Keep a single current interval per customer if one exists.
    SELECT
        customer_id,
        current_subscription_status,
        current_plan_id,
        current_plan_name,
        current_monthly_cost,
        current_subscription_start_date

    FROM latest_intervals
    WHERE current_interval_rank = 1
),

history_summary AS (
    -- Aggregate resolved subscription history across all intervals.
    SELECT
        customer_id,
        MIN(resolved_start_date)                                                          AS first_subscription_start_date,
        MAX(resolved_end_date)                                                            AS latest_subscription_end_date,
        MAX(CASE WHEN is_billable THEN 1 ELSE 0 END) = 1                                  AS has_ever_billable_subscription,
        MAX(CASE WHEN resolved_status = 'cancelled' THEN 1 ELSE 0 END) = 1                AS has_ever_cancelled,
        COUNT(*)                                                                          AS total_resolved_intervals

    FROM {{ ref('int_subscriptions_resolved') }}
    GROUP BY 1
),

final AS (
    SELECT
        hist.customer_id,
        cur.current_subscription_status,
        cur.current_plan_id,
        cur.current_plan_name,
        cur.current_monthly_cost,
        cur.current_subscription_start_date,
        hist.first_subscription_start_date,
        hist.latest_subscription_end_date,
        hist.has_ever_billable_subscription,
        hist.has_ever_cancelled,
        hist.total_resolved_intervals

    FROM history_summary                    AS hist
         LEFT JOIN current_one_per_customer AS cur ON hist.customer_id = cur.customer_id
)

SELECT *
FROM final

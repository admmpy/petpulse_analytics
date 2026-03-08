/*
This model creates a monthly recurring revenue fact table at customer-month grain.
It calculates prorated MRR for billable subscription periods after resolving
overlapping subscription records into a single customer timeline.
*/

{{
    config(
        materialized='table'
    )
}}

WITH RECURSIVE billable_intervals AS (
    -- Keep only revenue-bearing resolved subscription intervals.
    SELECT
        customer_id,
        resolved_plan_id,
        resolved_status,
        resolved_start_date,
        resolved_end_date,
        monthly_cost

    FROM {{ ref('int_subscriptions_resolved') }}
    WHERE is_billable = TRUE
),

date_limits AS (
    -- Fix a reproducible dataset horizon for open-ended intervals.
    SELECT
        MIN(DATE_TRUNC('month', resolved_start_date))                                     AS min_revenue_month,
        DATE_TRUNC(
            'month',
            COALESCE(MAX(resolved_end_date), MAX(resolved_start_date))
        )                                                                                 AS max_revenue_month,
        COALESCE(MAX(resolved_end_date), MAX(resolved_start_date))                        AS dataset_horizon_date

    FROM billable_intervals
),

month_spine (revenue_month) AS (
    -- Generate one row per calendar month across the resolved revenue horizon.
    SELECT
        min_revenue_month                                                                 AS revenue_month

    FROM date_limits

    UNION ALL

    SELECT
        CAST(revenue_month + INTERVAL 1 MONTH AS DATE)                                   AS revenue_month

    FROM month_spine
         CROSS JOIN date_limits
    WHERE revenue_month < max_revenue_month
),

month_windows AS (
    -- Attach month-end boundaries and month lengths once for later proration.
    SELECT
        revenue_month,
        CAST(revenue_month + INTERVAL 1 MONTH - INTERVAL 1 DAY AS DATE)                  AS month_end,
        DATEDIFF('day', revenue_month, CAST(revenue_month + INTERVAL 1 MONTH AS DATE))   AS days_in_month

    FROM month_spine
),

billable_intervals_horizon AS (
    -- Replace open-ended interval ends with the fixed dataset horizon.
    SELECT
        bill.customer_id,
        bill.resolved_plan_id,
        bill.resolved_status,
        bill.resolved_start_date,
        COALESCE(bill.resolved_end_date, lim.dataset_horizon_date)                        AS effective_end_date,
        bill.monthly_cost

    FROM billable_intervals     AS bill
         CROSS JOIN date_limits AS lim
),

interval_months AS (
    -- Keep only the customer-months where a billable interval overlaps the month.
    SELECT
        bill.customer_id,
        win.revenue_month,
        win.month_end,
        win.days_in_month,
        bill.resolved_start_date,
        bill.effective_end_date,
        bill.monthly_cost

    FROM billable_intervals_horizon AS bill
         INNER JOIN month_windows   AS win ON win.revenue_month <= DATE_TRUNC('month', bill.effective_end_date)
                                              AND win.month_end >= bill.resolved_start_date
),

interval_month_overlaps AS (
    -- Calculate the exact in-month overlap for each billable interval.
    SELECT
        customer_id,
        revenue_month,
        days_in_month,
        GREATEST(resolved_start_date, revenue_month)                                      AS overlap_start,
        LEAST(effective_end_date, month_end)                                              AS overlap_end,
        monthly_cost

    FROM interval_months
),

interval_month_revenue AS (
    -- Convert interval-month overlaps into active days and prorated MRR.
    SELECT
        customer_id,
        revenue_month,
        days_in_month,
        DATEDIFF('day', overlap_start, overlap_end) + 1                                   AS active_days_in_month,
        -- Proration happens here: monthly price times share of active days.
        monthly_cost
            * CAST(DATEDIFF('day', overlap_start, overlap_end) + 1 AS DOUBLE)
            / CAST(days_in_month AS DOUBLE)                                               AS prorated_mrr_amount

    FROM interval_month_overlaps
),

final AS (
    -- Multiple resolved intervals can exist within one month after plan changes.
    SELECT
        customer_id,
        revenue_month,
        SUM(active_days_in_month)                                                         AS active_days_in_month,
        MAX(days_in_month)                                                                AS days_in_month,
        ROUND(SUM(prorated_mrr_amount), 2)                                                AS mrr_amount

    FROM interval_month_revenue
    GROUP BY 1, 2
)

SELECT *
FROM final

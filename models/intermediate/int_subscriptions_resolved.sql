/*
This model creates a resolved subscription timeline for each customer.
It standardises subscription statuses and removes overlapping records by
selecting a single winning subscription interval at any point in time.
*/

{{
    config(
        materialized='view'
    )
}}

WITH base AS (
    -- Type raw subscription fields once and attach plan pricing.
    SELECT
        sub.subscription_id                                                               AS resolved_subscription_id,
        sub.customer_id,
        sub.plan_id                                                                       AS resolved_plan_id,
        LOWER(TRIM(sub.status_code))                                                      AS status_code_normalised,
        CAST(sub.subscription_start_date AS DATE)                                         AS start_date,
        CAST(sub.subscription_end_date AS DATE)                                           AS end_date_raw,
        pln.monthly_cost

    FROM {{ ref('raw_subscriptions') }}   AS sub
         LEFT JOIN {{ ref('raw_plans') }} AS pln ON sub.plan_id = pln.plan_id
),

subscriptions_clean AS (
    -- Normalise statuses and filter invalid rows before interval resolution.
    SELECT
        resolved_subscription_id,
        customer_id,
        resolved_plan_id,
        CASE
            WHEN status_code_normalised IN ('canceled', 'cancelled')
                THEN 'cancelled'
            WHEN status_code_normalised IN ('active', 'pending')
                THEN status_code_normalised
            ELSE NULL
        END                                                                               AS resolved_status,
        start_date,
        -- Carry open-ended intervals through resolution with a sentinel end date.
        COALESCE(end_date_raw, CAST('2999-12-31' AS DATE))                                AS end_date,
        monthly_cost,
        CASE
            WHEN status_code_normalised IN ('active', 'canceled', 'cancelled')
                THEN TRUE
            ELSE FALSE
        END                                                                               AS is_billable,
        CASE
            WHEN status_code_normalised = 'active' THEN 3
            WHEN status_code_normalised IN ('canceled', 'cancelled') THEN 2
            WHEN status_code_normalised = 'pending' THEN 1
            ELSE 0
        END                                                                               AS status_precedence

    FROM base
    WHERE status_code_normalised IN ('active', 'pending', 'canceled', 'cancelled')
          AND (
                end_date_raw IS NULL
                OR end_date_raw >= start_date
              )
),

boundaries AS (
    -- Emit all interval boundaries used to build atomic segments.
    SELECT
        customer_id,
        start_date                                                                        AS boundary_date

    FROM subscriptions_clean

    UNION ALL

    SELECT
        customer_id,
        CAST({{ dbt.dateadd('DAY', 1, 'end_date') }} AS DATE)                            AS boundary_date

    FROM subscriptions_clean
),

deduped_boundaries AS (
    -- Remove duplicate boundaries per customer.
    SELECT DISTINCT
        customer_id,
        boundary_date
    FROM boundaries
),

ordered_boundaries AS (
    -- Prepare each boundary alongside the next one in sequence.
    SELECT
        customer_id,
        boundary_date,
        LEAD(boundary_date) OVER (
            PARTITION BY customer_id
            ORDER BY boundary_date
        )                                                                                 AS next_boundary_date

    FROM deduped_boundaries
),

segments AS (
    -- Create atomic customer date segments from ordered boundaries.
    SELECT
        customer_id,
        boundary_date                                                                     AS segment_start,
        CAST({{ dbt.dateadd('DAY', -1, 'next_boundary_date') }} AS DATE)                 AS segment_end

    FROM ordered_boundaries
    WHERE next_boundary_date IS NOT NULL
),

winners AS (
    -- Resolve overlaps by applying deterministic precedence per customer-segment.
    SELECT
        seg.customer_id,
        seg.segment_start,
        seg.segment_end,
        sub.resolved_subscription_id,
        sub.resolved_plan_id,
        sub.resolved_status,
        sub.monthly_cost,
        sub.is_billable

    FROM segments AS seg
         INNER JOIN subscriptions_clean AS sub
             ON seg.customer_id = sub.customer_id
            AND seg.segment_start >= sub.start_date
            AND seg.segment_end <= sub.end_date

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY seg.customer_id, seg.segment_start, seg.segment_end
        ORDER BY
            -- Prefer billable states first, then the stronger business status.
            CASE WHEN sub.is_billable THEN 1 ELSE 0 END DESC,
            sub.status_precedence DESC,
            sub.start_date DESC,
            sub.monthly_cost DESC,
            sub.resolved_subscription_id ASC
    ) = 1
),

winner_sequence AS (
    -- Surface previous winning attributes once for interval coalescing.
    SELECT
        customer_id,
        resolved_subscription_id,
        resolved_plan_id,
        resolved_status,
        monthly_cost,
        is_billable,
        segment_start,
        segment_end,
        LAG(resolved_subscription_id) OVER (
            PARTITION BY customer_id
            ORDER BY segment_start, segment_end
        )                                                                                 AS prev_resolved_subscription_id,
        LAG(resolved_plan_id) OVER (
            PARTITION BY customer_id
            ORDER BY segment_start, segment_end
        )                                                                                 AS prev_resolved_plan_id,
        LAG(resolved_status) OVER (
            PARTITION BY customer_id
            ORDER BY segment_start, segment_end
        )                                                                                 AS prev_resolved_status,
        LAG(is_billable) OVER (
            PARTITION BY customer_id
            ORDER BY segment_start, segment_end
        )                                                                                 AS prev_is_billable,
        LAG(segment_end) OVER (
            PARTITION BY customer_id
            ORDER BY segment_start, segment_end
        )                                                                                 AS prev_segment_end

    FROM winners
),

group_flags AS (
    -- Mark where a new coalescing group starts.
    SELECT
        customer_id,
        resolved_subscription_id,
        resolved_plan_id,
        resolved_status,
        monthly_cost,
        is_billable,
        segment_start,
        segment_end,
        CASE
            WHEN prev_resolved_subscription_id = resolved_subscription_id
             AND prev_resolved_plan_id = resolved_plan_id
             AND prev_resolved_status = resolved_status
             AND prev_is_billable = is_billable
             AND prev_segment_end = CAST({{ dbt.dateadd('DAY', -1, 'segment_start') }} AS DATE)
                THEN 0
            ELSE 1
        END                                                                               AS new_group_flag

    FROM winner_sequence
),

grouped AS (
    -- Assign stable group ids to adjacent segments with unchanged winners.
    SELECT
        customer_id,
        resolved_subscription_id,
        resolved_plan_id,
        resolved_status,
        monthly_cost,
        is_billable,
        segment_start,
        segment_end,
        SUM(new_group_flag) OVER (
            PARTITION BY customer_id
            ORDER BY segment_start, segment_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                                                 AS segment_group_id

    FROM group_flags
),

final AS (
    -- Collapse adjacent winning segments back into analyst-friendly intervals.
    SELECT
        customer_id,
        resolved_subscription_id,
        resolved_plan_id,
        resolved_status,
        MIN(segment_start)                                                                AS resolved_start_date,
        CASE
            -- Restore open-ended intervals for downstream current-state logic.
            WHEN MAX(segment_end) = CAST('2999-12-31' AS DATE) THEN NULL
            ELSE MAX(segment_end)
        END                                                                               AS resolved_end_date,
        MAX(monthly_cost)                                                                 AS monthly_cost,
        is_billable

    FROM grouped
    GROUP BY
        customer_id,
        resolved_subscription_id,
        resolved_plan_id,
        resolved_status,
        is_billable,
        segment_group_id
)

SELECT *
FROM final

/*
This model creates a customer dimension at one row per customer.
It combines cleaned customer attributes with current subscription state
and lifetime value from the resolved subscription and revenue models.
*/

{{
    config(
        materialized='table'
    )
}}

WITH customer_lifetime_value AS (
    -- Aggregate prorated MRR into total lifetime value per customer.
    SELECT
        customer_id,
        ROUND(SUM(mrr_amount), 2)                                                         AS total_lifetime_value

    FROM {{ ref('fct_mrr_monthly') }}
    GROUP BY 1
),

final AS (
    SELECT
        cust.customer_id,
        cust.customer_name,
        cust.customer_email,
        cust.region,
        cust.signup_at,
        sub.current_subscription_status,
        -- Customers without billable history should still appear with zero value.
        COALESCE(ltv.total_lifetime_value, 0)                                             AS total_lifetime_value,
        cust.marketing_source,
        sub.current_plan_id,
        sub.current_plan_name,
        sub.current_monthly_cost,
        sub.current_subscription_start_date,
        sub.first_subscription_start_date,
        sub.latest_subscription_end_date,
        sub.has_ever_billable_subscription,
        sub.has_ever_cancelled

    FROM {{ ref('int_customers_enriched') }}                      AS cust
         LEFT JOIN {{ ref('int_customer_subscription_summary') }} AS sub ON cust.customer_id = sub.customer_id
         LEFT JOIN customer_lifetime_value                        AS ltv ON cust.customer_id = ltv.customer_id
)

SELECT *
FROM final

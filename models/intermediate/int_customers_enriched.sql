/*
This model creates a cleaned customer profile layer for downstream marts.
It standardises basic customer attributes such as name, email, region,
and marketing source from the supplied staged customer data.
*/

{{
    config(
        materialized='view'
    )
}}

WITH final AS (
    SELECT
        customer_id,
        TRIM(customer_name)                                                               AS customer_name,
        LOWER(TRIM(customer_email))                                                       AS customer_email,
        CAST(signup_at AS TIMESTAMP)                                                      AS signup_at,
        CASE
            WHEN TRIM(region_raw) = 'None' THEN NULL
            ELSE TRIM(region_raw)
        END                                                                               AS region,
        CASE
            WHEN TRIM(marketing_source_raw) = 'None' THEN NULL
            ELSE TRIM(marketing_source_raw)
        END                                                                               AS marketing_source

    FROM {{ ref('raw_customers') }}
)

SELECT *
FROM final

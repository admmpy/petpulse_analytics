SELECT
    customer_id,
    revenue_month,
    COUNT(*) AS row_count
    
FROM {{ ref('fct_mrr_monthly') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1

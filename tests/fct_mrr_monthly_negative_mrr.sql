SELECT *
FROM {{ ref('fct_mrr_monthly') }}
WHERE mrr_amount < 0

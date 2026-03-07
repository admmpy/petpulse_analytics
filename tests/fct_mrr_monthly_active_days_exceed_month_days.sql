SELECT *
FROM {{ ref('fct_mrr_monthly') }}
WHERE active_days_in_month > days_in_month

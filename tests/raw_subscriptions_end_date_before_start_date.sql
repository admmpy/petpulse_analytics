select *
from {{ ref('raw_subscriptions') }}
where subscription_end_date is not null
  and subscription_end_date < subscription_start_date

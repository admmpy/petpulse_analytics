select *
from {{ ref('raw_subscriptions') }}
where status_code not in ('ACTIVE', 'active', 'Canceled', 'Cancelled', 'pending')
    or status_code is null
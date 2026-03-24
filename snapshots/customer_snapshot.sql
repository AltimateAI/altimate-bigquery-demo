{% snapshot customer_snapshot %}

{{
    config(
        target_database='diesel-command-384802',
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='account_created_at',
        invalidate_hard_deletes=True,
    )
}}

select
    customer_id,
    email,
    full_name,
    age,
    gender,
    city,
    state,
    country,
    traffic_source,
    account_created_at,
    total_orders,
    total_revenue,
    customer_tier,
    activity_status
from {{ ref('dim_customers') }}

{% endsnapshot %}

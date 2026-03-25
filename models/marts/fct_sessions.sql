{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with sessions as (

    select * from {{ ref('int_session_metrics') }}

),

final as (

    select
        session_id,
        customer_id,
        session_start,
        session_end,
        timestamp_diff(session_end, session_start, minute) as session_duration_minutes,
        page_views,
        has_purchase,
        has_cart,
        traffic_source,
        browser,
        has_purchase as is_converted

    from sessions

)

select * from final

{% if is_incremental() %}
where revenue_date > (select max(revenue_date) from {{ this }})
{% endif %}

with events as (

    select * from {{ ref('stg_sessions') }}

),

session_metrics as (

    select
        session_id,
        min(customer_id)                                as customer_id,
        min(created_at)                                 as session_start,
        max(created_at)                                 as session_end,
        count(*)                                        as page_views,
        max(case when event_type = 'purchase' then true else false end) as has_purchase,
        max(case when event_type = 'cart' then true else false end)     as has_cart,
        min(traffic_source)                             as traffic_source,
        min(browser)                                    as browser,
        cast(min(created_at) as date)                   as session_date
    from events
    group by 1

)

select * from session_metrics

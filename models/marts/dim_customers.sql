with customers as (

    select * from {{ ref('stg_customers') }}

),

customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),

sessions as (

    select
        customer_id,
        count(*)                            as total_sessions,
        sum(page_views)                     as total_page_views,
        max(session_start)                  as last_session_at
    from {{ ref('int_session_metrics') }}
    where customer_id is not null
    group by 1

),

final as (

    select
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.full_name,
        c.age,
        c.gender,
        c.city,
        c.state,
        c.country,
        c.traffic_source,
        c.created_at                        as account_created_at,

        -- order metrics
        coalesce(co.total_orders, 0)        as total_orders,
        coalesce(co.total_revenue, 0)       as total_revenue,
        co.first_order_at,
        co.last_order_at,
        coalesce(co.avg_order_value, 0)     as avg_order_value,
        coalesce(co.total_items_purchased, 0) as total_items_purchased,

        -- session metrics
        coalesce(s.total_sessions, 0)       as total_sessions,
        coalesce(s.total_page_views, 0)     as total_page_views,
        s.last_session_at,

        -- derived classifications
        case
            when co.total_orders is null then 'prospect'
            when co.total_orders = 1 then 'new'
            when co.total_orders between 2 and 5 then 'active'
            when co.total_orders > 5 then 'loyal'
        end as customer_tier,

        case
            when co.last_order_at is null then 'never_purchased'
            when date_diff(current_date(), cast(co.last_order_at as date), day) <= 30 then 'active'
            when date_diff(current_date(), cast(co.last_order_at as date), day) <= 90 then 'at_risk'
            else 'churned'
        end as activity_status

    from customers c
    left join customer_orders co
        on c.customer_id = co.customer_id
    left join sessions s
        on c.customer_id = s.customer_id

)

select * from final

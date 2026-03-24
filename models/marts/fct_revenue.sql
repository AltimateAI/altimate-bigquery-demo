with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

daily as (

    select
        cast(oi.created_at as date)                as revenue_date,
        count(distinct oi.order_id)                as order_count,
        count(distinct oi.customer_id)             as unique_customers,
        sum(oi.sale_price)                         as daily_revenue,
        count(*)                                   as daily_items_sold,
        {{ safe_divide('sum(oi.sale_price)', 'count(distinct oi.order_id)') }} as avg_order_value

    from order_items oi
    group by 1

)

select * from daily

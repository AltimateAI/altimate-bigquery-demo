with orders as (

    select * from {{ ref('stg_orders') }}

),

order_metrics as (

    select * from {{ ref('int_order_metrics') }}

),

customer_orders as (

    select
        orders.customer_id,
        count(distinct orders.order_id)                 as total_orders,
        coalesce(sum(order_metrics.order_revenue), 0)   as total_revenue,
        min(orders.ordered_at)                          as first_order_at,
        max(orders.ordered_at)                          as last_order_at,
        {{ safe_divide(
            'coalesce(sum(order_metrics.order_revenue), 0)',
            'count(distinct orders.order_id)'
        ) }}                                            as avg_order_value,
        coalesce(sum(order_metrics.order_item_count), 0) as total_items_purchased
    from orders
    left join order_metrics
        on orders.order_id = order_metrics.order_id
    group by 1

)

select * from customer_orders

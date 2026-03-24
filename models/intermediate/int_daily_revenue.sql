with orders as (

    select * from {{ ref('stg_orders') }}

),

order_metrics as (

    select * from {{ ref('int_order_metrics') }}

),

daily_revenue as (

    select
        cast(o.ordered_at as date) as order_date,
        count(distinct o.order_id) as order_count,
        count(distinct o.customer_id) as unique_customers,
        coalesce(sum(om.order_revenue), 0) as gross_revenue,
        {{ safe_divide(
            'coalesce(sum(om.order_revenue), 0)',
            'count(distinct o.order_id)'
        ) }} as avg_order_value

    from orders o
    left join order_metrics om on o.order_id = om.order_id
    group by 1

)

select * from daily_revenue

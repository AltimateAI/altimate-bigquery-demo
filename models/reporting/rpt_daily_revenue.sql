with daily_revenue as (

    select * from {{ ref('fct_revenue') }}

),

final as (

    select
        revenue_date,
        order_count,
        unique_customers,
        daily_revenue,
        daily_items_sold,
        avg_order_value,
        sum(daily_revenue) over (
            order by revenue_date
            rows between 6 preceding and current row
        ) as revenue_7d_rolling,
        sum(order_count) over (
            order by revenue_date
            rows between 6 preceding and current row
        ) as orders_7d_rolling,
        sum(daily_revenue) over (
            order by revenue_date
            rows between 29 preceding and current row
        ) as revenue_30d_rolling

    from daily_revenue

)

select * from final

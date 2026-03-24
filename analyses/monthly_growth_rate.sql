/*
    Monthly revenue growth rate analysis.
    Calculates month-over-month revenue growth percentage.
*/

with monthly_revenue as (

    select
        date_trunc(revenue_date, month)     as revenue_month,
        sum(daily_revenue)                  as monthly_revenue,
        sum(order_count)                    as monthly_orders,
        sum(unique_customers)               as monthly_customers
    from {{ ref('fct_revenue') }}
    group by 1

),

with_growth as (

    select
        revenue_month,
        monthly_revenue,
        monthly_orders,
        monthly_customers,
        lag(monthly_revenue) over (order by revenue_month) as prev_month_revenue,
        lag(monthly_orders) over (order by revenue_month)  as prev_month_orders,
        safe_divide(
            monthly_revenue - lag(monthly_revenue) over (order by revenue_month),
            lag(monthly_revenue) over (order by revenue_month)
        ) * 100 as revenue_growth_pct,
        safe_divide(
            monthly_orders - lag(monthly_orders) over (order by revenue_month),
            lag(monthly_orders) over (order by revenue_month)
        ) * 100 as orders_growth_pct
    from monthly_revenue

)

select * from with_growth
order by revenue_month

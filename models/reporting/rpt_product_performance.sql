with products as (

    select * from {{ ref('dim_products') }}

),

final as (

    select
        product_id,
        product_name,
        category,
        brand,
        department,
        retail_price,
        cost,
        margin,
        total_units_sold,
        total_revenue,
        total_profit,
        order_count,
        sales_velocity,
        {{ safe_divide('total_profit', 'total_revenue') }} as profit_margin_pct,
        {{ safe_divide('total_revenue', 'nullif(order_count, 0)') }} as revenue_per_order,
        rank() over (partition by category order by total_revenue desc) as category_revenue_rank

    from products
    where total_orders > 0

)

select * from final

with products as (

    select * from {{ ref('stg_products') }}

),

product_sales as (

    select
        product_id,
        count(*) as total_units_sold,
        sum(sale_price) as total_revenue,
        sum(item_profit) as total_profit,
        count(distinct order_id) as order_count

    from {{ ref('int_order_items_enriched') }}
    group by product_id

),

final as (

    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.department,
        p.retail_price,
        p.cost,
        p.retail_price - p.cost as margin,
        p.distribution_center_id,
        coalesce(ps.total_units_sold, 0) as total_units_sold,
        coalesce(ps.total_revenue, 0) as total_revenue,
        coalesce(ps.total_profit, 0) as total_profit,
        coalesce(ps.order_count, 0) as order_count,
        case
            when ps.total_units_sold > 100 then 'high'
            when ps.total_units_sold > 20 then 'medium'
            else 'low'
        end as sales_velocity

    from products p
    left join product_sales ps on p.product_id = ps.product_id

)

select * from final

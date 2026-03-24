with order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

enriched as (

    select
        oi.order_item_id,
        oi.order_id,
        oi.customer_id,
        oi.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.department,
        oi.sale_price,
        p.cost as unit_cost,
        oi.sale_price - p.cost as item_profit,
        oi.order_status,
        oi.created_at,
        oi.shipped_at,
        oi.delivered_at,
        oi.returned_at

    from order_items oi
    inner join products p on oi.product_id = p.product_id

)

select * from enriched

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

order_metrics as (

    select
        order_id,
        min(customer_id)                    as customer_id,
        count(*)                            as order_item_count,
        sum(sale_price)                     as order_revenue_v2,
        {{ safe_divide('sum(sale_price)', 'count(*)') }} as avg_item_price
        generate_uuid() as run_id,
    from order_items
    group by 1

)

select * from order_metrics

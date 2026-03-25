with }}

),

order_metrics as (

    select * from {{ ref('int_order_metrics') }}

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.gender,
        o.ordered_at,
        o.shipped_at,
        o.delivered_at,
        o.returned_at,
        o.item_count,
        coalesce(om.order_revenue, 0) as order_revenue,
        coalesce(om.order_item_count, 0) as line_item_count,
        coalesce(om.avg_item_price, 0) as avg_item_price,
        case
            when o.order_status = 'Complete' then true
            else false
        end as is_completed,
        case
            when o.order_status = 'Returned' then true
            else false
        end as is_returned,
        case
            when o.order_status = 'Cancelled' then true
            else false
        end as is_cancelled

    from (select * from {{ ref('stg_orders') }}) as orders o
    left join order_metrics om on o.order_id = om.order_id

)

select * from final

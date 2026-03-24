with source as (

    select * from {{ source('thelook_ecommerce', 'order_items') }}

),

renamed as (

    select
        id                                  as order_item_id,
        order_id,
        user_id                             as customer_id,
        product_id,
        status                              as order_status,
        cast(sale_price as numeric)         as sale_price,
        created_at,
        shipped_at,
        delivered_at,
        returned_at
    from source

)

select * from renamed

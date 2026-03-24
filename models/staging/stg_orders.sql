with source as (

    select * from {{ source('thelook_ecommerce', 'orders') }}

),

renamed as (

    select
        order_id,
        user_id                             as customer_id,
        status                              as order_status,
        gender,
        created_at                          as ordered_at,
        returned_at,
        shipped_at,
        delivered_at,
        num_of_item                         as item_count
    from source

)

select * from renamed

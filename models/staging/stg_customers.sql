with source as (

    select * from {{ source('thelook_ecommerce', 'users') }}

),

renamed as (

    select
        id                                  as customer_id,
        lower(email)                        as email,
        first_name,
        last_name,
        concat(first_name, ' ', last_name)  as full_name,
        age,
        gender,
        city,
        state,
        country,
        traffic_source,
        created_at
    from source

)

select * from renamed

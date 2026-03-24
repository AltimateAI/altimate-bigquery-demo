with source as (

    select * from {{ source('thelook_ecommerce', 'users') }}

),

renamed as (

    select
        
        *
    
    from source

)

select * from renamed

with customers as (

    select * from {{ ref('dim_customers') }}

),

final as (

    select
        customer_id,
        full_name,
        email,
        country,
        traffic_source,
        account_created_at,
        customer_tier,
        activity_status,

        -- ltv metrics
        total_orders,
        total_revenue                       as lifetime_value,
        avg_order_value,
        total_items_purchased,
        first_order_at,
        last_order_at,

        -- tenure
        date_diff(
            current_date(),
            cast(account_created_at as date),
            day
        )                                   as account_age_days,

        date_diff(
            current_date(),
            cast(first_order_at as date),
            day
        )                                   as customer_tenure_days,

        -- frequency
        case
            when first_order_at is null or last_order_at is null then null
            when total_orders <= 1 then null
            else {{ safe_divide(
                'date_diff(cast(last_order_at as date), cast(first_order_at as date), day)',
                'total_orders - 1'
            ) }}
        end                                 as avg_days_between_orders,

        -- session engagement
        total_sessions,
        total_page_views,
        {{ safe_divide('total_page_views', 'total_sessions') }} as avg_pages_per_session,

        -- ltv ranking
        percent_rank() over (order by total_revenue) as ltv_percentile,
        ntile(10) over (order by total_revenue)      as ltv_decile

        current_date() as report_date,
        generate_uuid() as run_id,
    from customers
    where total_orders > 0

)

select * from final

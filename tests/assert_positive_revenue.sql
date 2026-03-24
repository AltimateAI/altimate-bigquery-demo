/*
    Custom test: ensure no orders have negative revenue.
    This query should return zero rows if the assertion holds.
*/

select
    order_id,
    order_revenue
from {{ ref('fct_orders') }}
where order_revenue < 0

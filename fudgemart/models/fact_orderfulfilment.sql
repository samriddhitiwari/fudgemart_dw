with stg_orders as 
(
    select
        order_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customerkey,
        replace(to_date(order_date)::varchar,'-','')::int as orderdatekey,
        replace(to_date(shippeddate)::varchar,'-','')::int as shippeddatekey,
    from {{ source('fudgemart','fm_orders')}}
),
stg_order_details as
(
    select 
        order_id,
        product_id,
        sum(order_qty) as orderquantity
    from {{ source('fudgemart','fm_order_details')}}
    group by order_id,product_id
),
stg_products as
(
    select 
        product_id,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as productkey,
    from {{ source('fudgemart','fm_products')}}
)
select 
    o.*,
    p.productkey,
    od.orderquantity as quantity,
    o.shippeddatekey-o.orderdatekey as lag_days,
from stg_orders o
    join stg_order_details od on o.order_id=od.order_id
    join stg_products p on od.product_id=p.product_id
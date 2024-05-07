with f_fact_orderfulfilment as (
    select * from {{ref("fact_orderfulfilment")}}
),
d_customer as (
    select * from {{ref("dim_customer")}}
),
d_product as (
    select * from {{ref('dim_product')}}
),
d_date as (
    select * from {{ref('dim_date')}}
)

select
    d_customer.*,
    d_product.*,
    d_date.*,
    f.quantity, f.lag_days
    
    from f_fact_orderfulfilment as f
    left join d_customer on f.customerkey = d_customer.customerkey
    left join d_product on f.productkey = d_product.productkey
    left join d_date on f.orderdatekey = d_date.datekey
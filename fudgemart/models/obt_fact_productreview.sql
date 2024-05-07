with f_fact_productreview as (
    select * from {{ref("fact_productreview")}}
),
d_customer as (
    select * from {{ref("dim_customer")}}
),
d_date as (
    select * from {{ref('dim_date')}}
)

select
    d_customer.*,
    d_date.*,
    f.product_name, f.reviewdatekey, f.productrating
    from f_fact_productreview as f
    left join d_customer on f.customerkey = d_customer.customerkey
    left join d_date on f.reviewdatekey = d_date.datekey
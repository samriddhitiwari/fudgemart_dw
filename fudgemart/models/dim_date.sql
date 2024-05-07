select     
    datekey::int as datekey,
    date,
    year,
    weekofyear,
    monthname,
    dayname
    from {{ source('conformed','DateDimension')}}
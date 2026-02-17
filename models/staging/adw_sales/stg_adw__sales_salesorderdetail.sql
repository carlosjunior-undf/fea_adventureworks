{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_salesorderdetail as (

    select * from {{ source('adw_sales', 'sales_salesorderdetail') }}

),

renamed as (

    select
        salesorderid
        ,salesorderdetailid
        ,carriertrackingnumber
        ,orderqty
        ,productid
        ,specialofferid
        ,unitprice
        ,unitpricediscount
        ,rowguid
        ,modifieddate

    from source_sales_salesorderdetail

)

select * from renamed
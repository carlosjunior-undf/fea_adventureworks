{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_customer as (

    select * from {{ source('adw_sales', 'sales_customer') }}

),

renamed as (

    select
        customerid
        ,personid
        ,storeid
        ,territoryid
        ,rowguid
        ,modifieddate

    from source_sales_customer

)

select * from renamed
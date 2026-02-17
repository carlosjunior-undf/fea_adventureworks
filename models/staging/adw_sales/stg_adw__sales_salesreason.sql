{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_salesreason as (

    select * from {{ source('adw_sales', 'sales_salesreason') }}

),

renamed as (

    select
        salesreasonid
        ,name
        ,reasontype
        ,modifieddate

    from source_sales_salesreason

)

select * from renamed
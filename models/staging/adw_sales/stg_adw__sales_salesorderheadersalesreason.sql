{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_salesorderheadersalesreason as (

    select * from {{ source('adw_sales', 'sales_salesorderheadersalesreason') }}

),

renamed as (

    select
        salesorderid
        ,salesreasonid
        ,modifieddate

    from source_sales_salesorderheadersalesreason

)

select * from renamed
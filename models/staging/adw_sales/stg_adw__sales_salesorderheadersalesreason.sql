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

        cast(salesorderid as int) as pedido_venda_id,
        cast(salesreasonid as int) as motivo_venda_id,
        --cast(modifieddate as date) as data_completa

    from source_sales_salesorderheadersalesreason

)
select * from renamed
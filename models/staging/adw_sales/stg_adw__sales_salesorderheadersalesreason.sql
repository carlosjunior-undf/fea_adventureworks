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
        {{ dbt_utils.generate_surrogate_key(['salesorderid', 'salesreasonid']) }} as motivo_venda_sk
        ,cast(salesorderid as int) as pedido_venda_fk
        ,cast(salesreasonid as int) as motivo_venda_fk
        ,cast(modifieddate as date) as data_completa

    from source_sales_salesorderheadersalesreason

)
select * from renamed
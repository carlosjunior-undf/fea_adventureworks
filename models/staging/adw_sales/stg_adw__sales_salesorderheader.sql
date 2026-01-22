{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_salesorderheader as (

    select * from {{ source('adw_sales', 'sales_salesorderheader') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid', 'customerid']) }} as pedido_venda_sk,
        cast(salesorderid as int) as pedido_venda_fk,
        cast(customerid as int) as cliente_fk,
        cast(salespersonid as int) as pessoa_venda_pk,
        cast(territoryid as int) as territorio_fk,
        cast(creditcardid as int) as cartao_credito_fk,
        cast(orderdate as date) as data_pedido,
        cast(shipdate as date) as data_envio,
        cast(modifieddate as date) as data_completa,
        cast(status as int) as codigo_status,
        subtotal,
        taxamt,
        freight,
        (subtotal + taxamt + freight) as faturamento_bruto

    from source_sales_salesorderheader

)

select * from renamed
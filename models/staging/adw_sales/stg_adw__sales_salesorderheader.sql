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
    
        cast(salesorderid as int) as pedido_venda_pk
        ,cast(customerid as int) as cliente_fk
        ,cast(territoryid as int) as territorio_fk
        ,cast(creditcardid as int) as cartao_credito_fk
        ,cast(orderdate as date) as data_pedido
        ,cast(duedate as date) as data_vencimento
        ,cast(shipdate as date) as data_envio
        ,cast(status as int) as status_pk
        ,cast(subtotal as float) as sub_total
        ,cast(taxamt as float) as taxa
        ,cast(freight as float) as frete
        ,cast(totaldue as float) as total_devido
        ,cast(modifieddate as date) as data_completa_pk
        --revisionnumber
        --onlineorderflag
        --purchaseordernumber
        --accountnumber
        --billtoaddressid
        --shiptoaddressid
        --shipmethodid
        --creditcardapprovalcode
        --currencyrateid
        --comment
        --rowguid
    from source_sales_salesorderheader

)
select * from renamed
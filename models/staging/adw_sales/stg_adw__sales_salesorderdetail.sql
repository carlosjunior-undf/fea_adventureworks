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
        cast(salesorderid as int) as pedido_venda_fk
        ,cast(productid as int) as produto_fk
        ,cast(salesorderdetailid as int) as detalhe_pedido_venda_pk
        ,cast(orderqty as int) as quantidade_comprada
        ,cast(unitprice as float) as preco_unitario
        ,cast(unitpricediscount as float) as desconto_unitario
        ,cast(modifieddate as date) as data_completa
        --specialofferid
        --carriertrackingnumber
        --rowguid
    from source_sales_salesorderdetail

)
select * from renamed
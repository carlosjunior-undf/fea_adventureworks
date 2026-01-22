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
        {{ dbt_utils.generate_surrogate_key(['salesorderid', 'salesorderdetailid', 'productid']) }} as pedido_venda_item_sk,
        cast(salesorderid as int) as pedido_venda_fk,
        cast(salesorderdetailid as int) as pedido_venda_item_fk,
        cast(productid as int) as produto_fk,
        cast(modifieddate as date) as data_completa,
        cast(orderqty as int) as quantidade_comprada,
        cast(unitprice as float) as preco_unitario,
        cast(unitpricediscount as float) as desconto,
        (preco_unitario * quantidade_comprada) as valor_total_negociado,
        (desconto * preco_unitario * quantidade_comprada) as desconto_pct
    from source_sales_salesorderdetail

)
select * from renamed
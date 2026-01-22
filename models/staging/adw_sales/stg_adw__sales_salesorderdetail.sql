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
        --cast(modifieddate as date) as modified_date,
        cast(orderqty as int) as quantidade_comprada,
        unitprice,
        unitpricediscount,
        (unitprice * orderqty) as valor_total_negociado,
        (unitpricediscount * unitprice * orderqty) as desconto
    from source_sales_salesorderdetail

)
select * from renamed
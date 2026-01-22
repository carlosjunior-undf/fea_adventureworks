{{ config(
    materialized="view",
    schema="int_adw"
) }}

with
    sales_salesorderheader as (
        select *
        from {{ ref('stg_adw__sales_salesorderheader') }}
    ),
    sales_salesorderdetail as (
        select *
        from {{ ref('stg_adw__sales_salesorderdetail') }}
    ), 
    sales_salesorderheadersalesreason as (
        select *
        from {{ ref('stg_adw__sales_salesorderheadersalesreason') }}
    ),
        sales_sales_customer as (
        select *
        from {{ ref('stg_adw__sales_customer') }}
    ),
    -- transformation
    joined as (
        select
            sales_salesorderheader.pedido_venda_sk,
            sales_salesorderheader.pedido_venda_fk,
            sales_salesorderheader.pessoa_venda_pk,
            sales_salesorderheader.cliente_fk,
            sales_salesorderheader.territorio_fk,
            sales_salesorderheader.cartao_credito_fk,
            sales_salesorderheader.data_pedido,
            sales_salesorderheader.data_envio,
            sales_salesorderheader.data_completa,
            sales_salesorderheader.codigo_status,
            sales_salesorderheader.sub_total,
            sales_salesorderheader.taxa,
            sales_salesorderheader.frete,

            sales_salesorderdetail.pedido_venda_item_sk,
            sales_salesorderdetail.data_completa,
            sales_salesorderdetail.quantidade_comprada,
            sales_salesorderdetail.preco_unitario,
            sales_salesorderdetail.desconto_pct,

            sales_salesorderheadersalesreason.pedido_venda_pk
            sales_salesorderheadersalesreason.motivo_venda_fk
            sales_salesorderheadersalesreason.data_completa

            sales_customer.cliente_pk
            sales_customer.pessoa_pk
            sales_customer.territorio_fk
            sales_customer.data_completa
        from sales_salesorderheader
--        inner join production_productsubcategory on production_product.subcategoria_fk = production_productsubcategory.subcategoria_pk
--        inner join production_productcategory on production_productsubcategory.categoria_fk = production_productcategory.categoria_pk
    )
select * from joined
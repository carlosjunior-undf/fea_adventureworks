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

    joined as (
        select
            sales_salesorderdetail.ordem_item_sk
            ,sales_salesorderdetail.pedido_venda_fk
            ,sales_salesorderdetail.produto_fk
            ,sales_salesorderdetail.detalhe_pedido_venda_pk
            ,sales_salesorderdetail.quantidade_comprada
            ,sales_salesorderdetail.preco_unitario
            ,sales_salesorderdetail.desconto_unitario
--            ,sales_salesorderdetail.data_completa

            ,sales_salesorderheader.pedido_venda_pk
            ,sales_salesorderheader.pessoa_venda_pk
            ,sales_salesorderheader.cliente_fk
            ,sales_salesorderheader.territorio_fk
            ,sales_salesorderheader.cartao_credito_fk
            ,sales_salesorderheader.data_pedido
            ,sales_salesorderheader.data_vencimento
            ,sales_salesorderheader.data_envio
            ,sales_salesorderheader.codigo_status
            ,sales_salesorderheader.sub_total
            ,sales_salesorderheader.taxa
            ,sales_salesorderheader.frete
            ,sales_salesorderheader.total_devido
            ,sales_salesorderheader.data_completa

        from sales_salesorderheader
        inner join sales_salesorderdetail on sales_salesorderheader.pedido_venda_pk = sales_salesorderdetail.pedido_venda_fk

    )
select * from joined

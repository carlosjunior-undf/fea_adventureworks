{{ config(
    materialized="view",
    schema="int_adw"
) }}
with

    sales_salesorderheadersalesreason as (
        select *
        from {{ ref('stg_adw__sales_salesorderheadersalesreason') }}
    ), 

    sales_salesreason as (
        select *
        from {{ ref('stg_adw__sales_salesreason') }}
    ),

    joined as (
        select
            sales_salesorderheadersalesreason.motivo_venda_sk
            ,sales_salesorderheadersalesreason.pedido_venda_fk
            ,sales_salesorderheadersalesreason.motivo_venda_fk
--            ,sales_salesorderheadersalesreason.data_completa

            ,sales_salesreason.motivo_venda_pk
            ,sales_salesreason.nome_motivo
            ,sales_salesreason.tipo_motivo
            ,sales_salesreason.data_completa

        from sales_salesreason
        inner join sales_salesorderheadersalesreason on sales_salesreason.motivo_venda_pk = sales_salesorderheadersalesreason.motivo_venda_fk
    )
select * from joined
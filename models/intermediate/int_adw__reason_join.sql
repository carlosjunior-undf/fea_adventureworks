{{ config(
    materialized="view",
    schema="int_adw"
) }}
with
    sales_salesreason as (
        select *
        from {{ ref('stg_adw__sales_salesreason') }}
    ),
    sales_salesorderheadersalesreason as (
        select *
        from {{ ref('stg_adw__sales_salesorderheadersalesreason') }}
    ), 

    joined as (
        select
            sales_salesorderheadersalesreason.motivo_venda_sk,
            sales_salesorderheadersalesreason.pedido_venda_fk,
            sales_salesorderheadersalesreason.motivo_venda_fk,
--            sales_salesorderheadersalesreason.data_completa,

            sales_salesreason.motivo_venda_pk,
            sales_salesreason.nome_motivo,
            sales_salesreason.tipo_motivo
--            sales_salesreason.data_completa

        from sales_salesorderheadersalesreason
        inner join sales_salesreason on sales_salesorderheadersalesreason.motivo_venda_fk = sales_salesreason.motivo_venda_pk
    )
select * from joined
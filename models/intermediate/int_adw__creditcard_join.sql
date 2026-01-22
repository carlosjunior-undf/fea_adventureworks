{{ config(
    materialized="view",
    schema="int_adw"
) }}
with
    sales_creditcard as (
        select *
        from {{ ref('stg_adw__sales_creditcard') }}
    ),
    sales_personcreditcard as (
        select *
        from {{ ref('stg_adw__sales_personcreditcard') }}
    ), 

    joined as (
        select

            sales_personcreditcard.cartao_credito_sk,
            sales_personcreditcard.entidade_empresa_fk,
            sales_personcreditcard.cartao_credito_fk,

            sales_creditcard.cartao_credito_pk,
            sales_creditcard.tipo_cartao,
            sales_creditcard.numero_cartao

        from sales_creditcard
        inner join sales_personcreditcard on sales_creditcard.cartao_credito_pk = sales_personcreditcard.cartao_credito_fk
    )
select * from joined
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

    -- Após realizar dos joins, copie todas as colunas para a tabela de dimensão correspondente.
    
    joined as (
        select

            sales_creditcard.cartao_credito_sk
            ,sales_creditcard.cartao_credito_pk
            ,sales_creditcard.tipo_cartao
            ,sales_creditcard.numero_cartao
            ,sales_creditcard.data_completa

            ,sales_personcreditcard.cliente_fk
            ,sales_personcreditcard.cartao_credito_fk
            --sales_personcreditcard.data_completa

        from sales_creditcard
        inner join sales_personcreditcard on sales_creditcard.cartao_credito_pk = sales_personcreditcard.cartao_credito_fk

    )
select * from joined
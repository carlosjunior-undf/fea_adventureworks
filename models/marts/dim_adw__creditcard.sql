{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with
    int_creditcard as (
        select *
        from {{ ref('int_adw__creditcard_join') }}
    ),
        -- Copie e cole todas as colunas da int_adw__customer_join, mas exclua as colunas com as chaves (PK e FK);
    -- Deixe apenas a chave SK e as demais colunas ativas;
    -- Copie e cole a chave SK na tabela fato.
    creditcard_transformed as (
        select

            cartao_credito_sk
            ,cartao_credito_pk
            ,cliente_fk
            ,tipo_cartao
            ,numero_cartao
            ,data_completa

        from int_creditcard
    )
select * from creditcard_transformed
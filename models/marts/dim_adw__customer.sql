{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_customer as (
        select *
        from {{ ref('int_adw__customer_join') }}
    ),
        -- Copie e cole todas as colunas da int_adw__customer_join, mas exclua as colunas com as chaves (PK e FK);
    -- Deixe apenas a chave SK e as demais colunas ativas;
    -- Copie e cole a chave SK na tabela fato.
    customer_transformed as (
        select

            (cliente_sk) as cliente_fk
            ,nome_pessoa
            ,email_pessoa
            ,telefone_pessoa
            ,numero_cartao
            ,tipo_cartao
            ,data_completa

        from int_customer
    )
select * from customer_transformed
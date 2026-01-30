{{ config(
    materialized="table",
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

            cliente_sk
            ,cliente_fk
            ,nome_pessoa
            ,telefone_pessoa
            ,endereco_pessoa
            ,cidade_pessoa
            ,cep_pessoa
            ,email_pessoa
            ,data_completa

        from int_customer
    )
select * from customer_transformed
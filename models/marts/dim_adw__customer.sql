{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_customer as (
        select *
        from {{ ref('int_adw__customer_join') }}
    ),
    
    dim_adw_customer__metrics as (
        select
            entidade_empresa_sk,
            nome_cliente,
            endereco_pessoa,
            numero_telefone,
            --cidade_pessoa,
            email_pessoa
        from int_customer
    )
select * from dim_adw_customer__metrics
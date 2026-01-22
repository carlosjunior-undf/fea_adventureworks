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
            entidade_empresa_fk,
            nome_cliente,
            email_pessoa,
            numero_telefone
        from int_customer
    )
select * from dim_adw_customer__metrics
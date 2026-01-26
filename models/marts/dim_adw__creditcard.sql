{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_creditcard as (
        select *
        from {{ ref('int_adw__creditcard_join') }}
    ),
    creditcard_transformed as (
        select
            {{ dbt_utils.generate_surrogate_key(['entidade_pessoa_id', 'cartao_credito_id']) }} as cartao_credito_sk,
            entidade_pessoa_id,
            cartao_credito_id,
            
            tipo_cartao,
            numero_cartao
        from int_creditcard
    )
select * from creditcard_transformed
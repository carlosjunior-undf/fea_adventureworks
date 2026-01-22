{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    stg_creditcard as (
        select *
        from {{ ref('int_adw__creditcard_join') }}
    ),
    
    dim_adw_creditcard__metrics as (
        select
            cartao_credito_sk,
            tipo_cartao,
            numero_cartao
        from stg_creditcard
    )
select * from dim_adw_creditcard__metrics
{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    stg_creditcard as (
        select *
        from {{ ref('stg_adw__sales_creditcard') }}
    ),
    
    dim_adw_creditcard__metrics as (
        select
            cartao_credito_sk,
            tipo_cartao
        from stg_creditcard
    )
select * from dim_adw_creditcard__metrics
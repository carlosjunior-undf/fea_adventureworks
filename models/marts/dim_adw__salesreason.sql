{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    stg_salesreason as (
        select *
        from {{ ref('stg_adw__sales_salesreason') }}
    ),
    
    dim_adw_salesreason__metrics as (
        select
            motivo_venda_sk,
            nome_motivo,
            tipo_motivo
        from stg_salesreason
    )
select * from dim_adw_salesreason__metrics
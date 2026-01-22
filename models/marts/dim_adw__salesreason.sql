{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_salesreason as (
        select *
        from {{ ref('int_adw__reason_join') }}
    ),
    
    dim_adw_salesreason__metrics as (
        select
            motivo_venda_sk,
            nome_motivo,
            tipo_motivo
        from int_salesreason
    )
select * from dim_adw_salesreason__metrics
{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_salesreason as (
        select *
        from {{ ref('int_adw__reason_join') }}
    ),
    
    -- Traga todas as colunas da int_adw__reason_join, mas deixe apenas a SK e as demais colunas ativas.
    reason_transformed as (
        select

            motivo_venda_sk
            ,nome_motivo
            ,tipo_motivo
            ,data_completa

        from int_salesreason
    )
select * from reason_transformed
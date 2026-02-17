{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with
    sales_reason as (
        select *
        from {{ ref('stg_adw__sales_salesreason') }}
    ),
    reason_transformed as (
        select

        {{ dbt_utils.generate_surrogate_key(['salesreasonid']) }} as motivo_venda_sk
        ,salesreasonid as motivo_venda_pk
        ,name as nome_motivo
        ,reasontype as tipo_motivo

        from sales_reason
    )
select * from reason_transformed
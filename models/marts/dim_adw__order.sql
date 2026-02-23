-- dim_adw__order.sql
-- Dimensão de pedidos: um registro por salesorderid.
-- Objetivo exclusivo: ser o lado "um" do relacionamento com bridge_adw__salesreason no Power BI,
-- resolvendo o problema de duplicidade de salesorderid na fct_adw__sales.
--
-- Apenas order_sk e salesorderid são necessários aqui.
-- Todos os demais atributos do pedido (datas, status, cliente, cartão) já estão na fct_adw__sales
-- via suas respectivas dimensões.
--
-- Relacionamento no Power BI:
--   fct_adw__sales   (N) ── order_sk ──(1) dim_adw__order
--   dim_adw__order   (1) ── order_sk ──(N) bridge_adw__salesreason  [filtro: Ambos]
--   bridge           (N) ── salesreason_sk ──(1) dim_adw__salesreason

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with order_header as (

    select salesorderid
    from {{ ref('stg_adw__sales_salesorderheader') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid']) }}  as order_sk
        ,salesorderid

    from order_header

)

select * from final
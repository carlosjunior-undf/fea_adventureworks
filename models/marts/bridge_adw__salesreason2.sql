-- bridge_adw__salesreason.sql
-- Tabela bridge para relação N:N entre fct_adw__sales e dim_adw__salesreason.
-- Permite análise de múltiplos motivos por pedido no Power BI sem duplicar métricas.

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with order_reason as (

    select * from {{ ref('int_adw__salesreason2') }}

),

dim_salesreason as (

    select salesreasonid, salesreason_sk
    from {{ ref('dim_adw__salesreason2') }}

),

-- granularidade: uma linha por salesorderid + salesreasonid
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_reason.salesorderid', 'order_reason.salesreasonid']) }} as bridge_sk
        ,order_reason.salesorderid
        ,dim_salesreason.salesreason_sk

    from order_reason
    inner join dim_salesreason on order_reason.salesreasonid = dim_salesreason.salesreasonid

)

select * from final
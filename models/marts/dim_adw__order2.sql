-- dim_adw__order.sql
-- Dimensão de Pedidos: um registro por salesorderid (único).
-- Objetivo: ser o lado "um" no relacionamento com bridge_adw__salesreason no Power BI.
--
-- Com este modelo:
--   - fct → dim_order: N:1, filtro Único (sem risco de duplicidade)
--   - dim_order → bridge: 1:N, filtro Único (dim_order é o lado único)
--   - bridge → dim_salesreason: N:1, filtro Único

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with order_header as (

    select * from {{ ref('stg_adw__sales_salesorderheader') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid']) }}  as order_sk
        ,salesorderid
        ,orderdate
        ,duedate
        ,shipdate
        ,status
        ,onlineorderflag
        ,customerid
        ,creditcardid
        ,subtotal
        ,taxamt
        ,freight
        ,totaldue

    from order_header

)

select * from final
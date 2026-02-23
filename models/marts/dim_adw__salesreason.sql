-- dim_adw__salesreason.sql
-- Nota: salesreason tem relação N:N com salesorder (um pedido pode ter vários motivos).
-- Esta dimensão representa os motivos únicos; a relação com o pedido é feita na bridge
-- que está embutida na fct via join direto no intermediate.

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with source as (

    select * from {{ ref('stg_adw__sales_salesreason') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesreasonid']) }} as salesreason_sk
        ,salesreasonid
        ,name as salesreason_name
        ,reasontype

    from source

)

select * from final
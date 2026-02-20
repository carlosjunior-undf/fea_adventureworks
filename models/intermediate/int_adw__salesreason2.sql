{{ config(
    materialized="view",
    schema="int_adw"
) }}
-- int_adw__salesreason.sql
-- Camada intermediate: join entre salesreason e o mapeamento order/reason

with salesreason as (

    select * from {{ ref('stg_adw__sales_salesreason') }}

),

order_reason as (

    select * from {{ ref('stg_adw__sales_salesorderheadersalesreason') }}

),

joined as (

    select
        order_reason.salesorderid

        ,salesreason.salesreasonid
        ,(salesreason.name) as salesreason_name
        ,salesreason.reasontype

    from order_reason
    inner join salesreason on order_reason.salesreasonid = salesreason.salesreasonid

)

select * from joined
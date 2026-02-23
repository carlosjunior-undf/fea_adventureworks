-- int_adw__salesreason.sql
-- Camada intermediate: join entre salesreason e o mapeamento order/reason

{{ config(
    materialized="view",
    schema="int_adw"
) }}

with salesreason as (

    select * from {{ ref('stg_adw__sales_salesreason') }}

),

order_reason as (

    select * from {{ ref('stg_adw__sales_salesorderheadersalesreason') }}

),

joined as (

    select
        orr.salesorderid
        ,sr.salesreasonid
        ,sr.name        as salesreason_name
        ,sr.reasontype

    from order_reason       orr
    inner join salesreason  sr on orr.salesreasonid = sr.salesreasonid

)

select * from joined

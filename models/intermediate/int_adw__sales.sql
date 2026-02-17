{{ config(
    materialized="view",
    schema="int_adw"
) }}

with detail_order as (
    select *
    from {{ ref('stg_adw__sales_salesorderdetail') }}
),

header_order as (
    select *
    from {{ ref('stg_adw__sales_salesorderheader') }}
),

salesreason as (
    select
        salesorderid
        ,min(salesreasonid) as salesreasonid
    from {{ ref('stg_adw__sales_salesorderheadersalesreason') }}
    group by salesorderid
)

select
    detail_order.salesorderid
    ,detail_order.salesorderdetailid
    ,detail_order.productid
    ,header_order.customerid
    ,header_order.creditcardid
    ,header_order.status
    ,header_order.orderdate
    ,salesreason.salesreasonid

    ,detail_order.orderqty
    ,detail_order.unitprice
    ,detail_order.unitpricediscount

from detail_order
left join header_order on detail_order.salesorderid = header_order.salesorderid
left join salesreason on detail_order.salesorderid = salesreason.salesorderid

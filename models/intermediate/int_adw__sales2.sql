{{ config(
    materialized="view",
    schema="int_adw"
) }}
-- int_adw__sales.sql
-- Camada intermediate: monta o grain da fato em nível de detalhe de pedido (salesorderdetail).
-- A relação N:N entre pedido e motivo de venda é resolvida aqui:
--   - agrega os motivos em um campo de lista (para fins analíticos)
--   - e mantém o salesreasonid "primário" (menor id) para permitir FK simples na fato.
-- Se quiser análise completa por motivo, use a bridge table bridge_adw__salesreason
-- que também é gerada neste projeto.

with order_header as (

    select * from {{ ref('stg_adw__sales_salesorderheader') }}

),

order_detail as (

    select * from {{ ref('stg_adw__sales_salesorderdetail') }}

),

order_reason as (

    select * from {{ ref('int_adw__salesreason2') }}

),

-- agrega motivos por pedido: pega o menor salesreasonid como "primário"
reason_per_order as (

    select
        salesorderid
        ,min(salesreasonid) as primary_salesreasonid
        ,collect_list(salesreason_name) as salesreason_list

    from order_reason
    group by salesorderid

),

joined as (

    select
        order_detail.salesorderid
        ,order_detail.salesorderdetailid

        ,order_header.orderdate
        ,order_header.duedate
        ,order_header.shipdate
        ,order_header.status
        ,order_header.customerid
        ,order_header.salespersonid
        ,order_header.territoryid
        ,order_header.billtoaddressid
        ,order_header.shiptoaddressid
        ,order_header.creditcardid

        ,order_detail.productid
        ,order_detail.specialofferid
        ,order_detail.orderqty
        ,order_detail.unitprice
        ,order_detail.unitpricediscount
        ,round(order_detail.orderqty * order_detail.unitprice, 2) as gross_revenue
        ,round(order_detail.orderqty * order_detail.unitprice * order_detail.unitpricediscount, 2) as discount_amount
        ,round(order_detail.orderqty * order_detail.unitprice * (1 - order_detail.unitpricediscount), 2) as net_revenue

        ,order_header.subtotal
        ,order_header.taxamt
        ,order_header.freight
        ,order_header.totaldue

        ,coalesce(reason_per_order.primary_salesreasonid, -1) as primary_salesreasonid
        -- -1 = pedido sem motivo cadastrado

    from order_detail
    inner join order_header on order_detail.salesorderid = order_header.salesorderid
    left join  reason_per_order on order_detail.salesorderid = reason_per_order.salesorderid

)

select * from joined
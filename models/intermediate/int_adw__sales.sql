-- int_adw__sales.sql
-- Camada intermediate: monta o grain da fato em nível de detalhe de pedido (salesorderdetail).
--
-- Responsabilidades:
--   1. JOIN entre order_detail (grain) e order_header (contexto do pedido)
--   2. Cálculo das métricas de receita (gross_revenue, discount_amount, net_revenue)
--
{{ config(
    materialized="view",
    schema="int_adw"
) }}

with order_header as (

    select * from {{ ref('stg_adw__sales_salesorderheader') }}

),

order_detail as (

    select * from {{ ref('stg_adw__sales_salesorderdetail') }}

),

final as (

    select
        -- grain da fato
        order_detail.salesorderid
        ,order_detail.salesorderdetailid

        -- atributos do cabeçalho — necessários para JOINs na fct_adw__sales
        ,order_header.orderdate
        ,order_header.status
        ,order_header.customerid
        ,order_header.creditcardid

        -- atributo da linha de detalhe
        ,order_detail.productid
        ,order_detail.orderqty

        -- métricas calculadas
        -- unitprice e unitpricediscount usados apenas nos cálculos abaixo, não expostos
        ,round(order_detail.orderqty * order_detail.unitprice, 2)
            as gross_revenue
        ,round(order_detail.orderqty * order_detail.unitprice * order_detail.unitpricediscount, 2)
            as discount_amount
        ,round(order_detail.orderqty * order_detail.unitprice * (1 - order_detail.unitpricediscount), 2)
            as net_revenue

    from order_detail
    inner join order_header
        on order_detail.salesorderid = order_header.salesorderid

)

select * from final
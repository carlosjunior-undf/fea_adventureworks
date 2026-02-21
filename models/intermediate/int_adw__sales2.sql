-- int_adw__sales.sql
-- Camada intermediate: monta o grain da fato em nível de detalhe de pedido (salesorderdetail).
--
-- Responsabilidades deste modelo:
--   1. JOIN entre order_detail (grain) e order_header (contexto do pedido)
--   2. Cálculo das métricas de receita na linha de detalhe
--   3. Resolução do motivo de venda primário por pedido (menor salesreasonid)
--      → usado como FK simples na fct_adw__sales (primary_salesreason_sk)
--      → para análise completa por motivo, use bridge_adw__salesreason
--
-- Campos do order_header que pertencem à dimensão de pedido (dim_adw__order)
-- são mantidos aqui para que a fct_adw__sales possa fazer o JOIN com dim_adw__order.
-- Não há duplicidade: dim_adw__order é gerada direto do staging, independentemente.
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

order_reason as (

    select * from {{ ref('int_adw__salesreason2') }}

),

-- agrega motivos por pedido: pega o menor salesreasonid como "primário"
-- para uso na FK simples primary_salesreason_sk da fct_adw__sales
reason_per_order as (

    select
        salesorderid
        ,min(salesreasonid) as primary_salesreasonid

    from order_reason
    group by salesorderid

),

joined as (

    select
        -- chaves do grain da fato
        order_detail.salesorderid
        ,order_detail.salesorderdetailid

        -- campos do cabeçalho do pedido
        -- (mantidos para JOINs com dim_adw__order, dim_date, dim_status, etc. na fato)
        ,order_header.orderdate
        ,order_header.duedate
        ,order_header.shipdate
        ,order_header.status
        ,order_header.customerid
        ,order_header.creditcardid

        -- campos da linha de detalhe
        ,order_detail.productid
        ,order_detail.orderqty
        ,order_detail.unitprice
        ,order_detail.unitpricediscount

        -- métricas calculadas na linha de detalhe
        ,round(order_detail.orderqty * order_detail.unitprice, 2)
            as gross_revenue
        ,round(order_detail.orderqty * order_detail.unitprice * order_detail.unitpricediscount, 2)
            as discount_amount
        ,round(order_detail.orderqty * order_detail.unitprice * (1 - order_detail.unitpricediscount), 2)
            as net_revenue

        -- campos financeiros do cabeçalho (nível de pedido — para referência)
        ,order_header.taxamt
        ,order_header.freight

        -- motivo de venda primário: -1 quando o pedido não possui motivo cadastrado
        ,coalesce(reason_per_order.primary_salesreasonid, -1) as primary_salesreasonid

    from order_detail
    inner join order_header
        on order_detail.salesorderid = order_header.salesorderid
    left join reason_per_order
        on order_detail.salesorderid = reason_per_order.salesorderid

)

select * from joined
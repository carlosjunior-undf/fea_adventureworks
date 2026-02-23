-- fct_adw__sales.sql
-- Tabela fato central de vendas da Adventure Works.
-- Granularidade: uma linha por salesorderdetailid (linha de detalhe do pedido).
--
-- Responde às perguntas de negócio A–F em conjunto com as dimensões.
-- Análise por motivo de venda: usar bridge_adw__salesreason via dim_adw__order.
--
-- ─── Relacionamentos no Power BI ──────────────────────────────────────────────
--   fct_adw__sales (N) ──── order_sk      ──(1) dim_adw__order        [Único]
--   fct_adw__sales (N) ──── customer_sk   ──(1) dim_adw__customer     [Único]
--   fct_adw__sales (N) ──── product_sk    ──(1) dim_adw__product      [Único]
--   fct_adw__sales (N) ──── creditcard_sk ──(1) dim_adw__creditcard   [Único]
--   fct_adw__sales (N) ──── status_sk     ──(1) dim_adw__status       [Único]
--   fct_adw__sales (N) ──── order_date_sk ──(1) dim_adw__date         [Único]
--   dim_adw__order (1) ──── order_sk      ──(N) bridge                [AMBOS]
--   bridge         (N) ──── salesreason_sk──(1) dim_adw__salesreason  [Único]
-- ──────────────────────────────────────────────────────────────────────────────

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with int_sales as (

    select * from {{ ref('int_adw__sales') }}

),

dim_customer as (

    select customerid, customer_sk
    from {{ ref('dim_adw__customer') }}

),

dim_product as (

    select productid, product_sk
    from {{ ref('dim_adw__product') }}

),

dim_creditcard as (

    select creditcardid, creditcard_sk
    from {{ ref('dim_adw__creditcard') }}

),

dim_status as (

    select statusid, status_sk
    from {{ ref('dim_adw__status') }}

),

dim_date as (

    select full_date, date_sk
    from {{ ref('dim_adw__date') }}

),

dim_order as (

    select salesorderid, order_sk
    from {{ ref('dim_adw__order') }}

),

final as (

    select
        -- surrogate key da fato
        {{ dbt_utils.generate_surrogate_key(['int_sales.salesorderid', 'int_sales.salesorderdetailid']) }}
            as sales_sk

        -- FKs de rastreabilidade (não usar em relacionamentos do Power BI)
        ,int_sales.salesorderid
        ,int_sales.salesorderdetailid

        -- FK hub → conecta à bridge via dim_adw__order no Power BI
        ,dim_order.order_sk

        -- FKs para dimensões analíticas
        ,dim_customer.customer_sk
        ,dim_product.product_sk
        ,dim_creditcard.creditcard_sk   -- pode ser nulo (pedidos sem cartão)
        ,dim_status.status_sk
        ,dim_date.date_sk               as order_date_sk

        -- métricas de negócio
        ,int_sales.orderqty
        ,int_sales.gross_revenue
        ,int_sales.discount_amount
        ,int_sales.net_revenue

        -- data nativa: facilita filtros rápidos no Power BI sem JOIN com dim_date
        ,int_sales.orderdate

    from int_sales
    inner join dim_date
        on cast(int_sales.orderdate as date) = dim_date.full_date
    left  join dim_order
        on int_sales.salesorderid            = dim_order.salesorderid
    left  join dim_customer
        on int_sales.customerid              = dim_customer.customerid
    left  join dim_product
        on int_sales.productid               = dim_product.productid
    left  join dim_creditcard
        on int_sales.creditcardid            = dim_creditcard.creditcardid
    left  join dim_status
        on int_sales.status                  = dim_status.statusid

)

select * from final

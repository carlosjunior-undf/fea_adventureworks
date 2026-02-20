-- fct_adw__sales.sql
-- Tabela fato de vendas com granularidade: 1 linha por linha de pedido (salesorderdetail).
-- Conecta todas as dimensões via surrogate keys.

{{ config(
    materialized = "table",
    schema = "marts_adw"
) }}

with int_sales as (

    select * from {{ ref('int_adw__sales2') }}

),

dim_customer as (

    select customerid, customer_sk
    from {{ ref('dim_adw__customer2') }}

),

dim_product as (

    select productid, product_sk
    from {{ ref('dim_adw__product2') }}

),

dim_creditcard as (

    select creditcardid, creditcard_sk
    from {{ ref('dim_adw__creditcard2') }}

),

dim_status as (

    select statusid, status_sk
    from {{ ref('dim_adw__status2') }}

),

dim_date as (

    select full_date, date_sk
    from {{ ref('dim_adw__date2') }}

),

dim_salesreason as (

    select salesreasonid, salesreason_sk
    from {{ ref('dim_adw__salesreason2') }}

),

final as (

    select
        -- surrogate key da fato
        {{ dbt_utils.generate_surrogate_key(['int_sales.salesorderid', 'int_sales.salesorderdetailid']) }}  as sales_sk

        -- chaves naturais (para rastreabilidade)
        ,int_sales.salesorderid
        ,int_sales.salesorderdetailid

        -- foreign keys para dimensões
        ,dim_customer.customer_sk

        ,dim_product.product_sk

        ,dim_creditcard.creditcard_sk

        ,dim_status.status_sk
        
        ,dd_order.date_sk as order_date_sk
        ,dd_ship.date_sk as ship_date_sk
        ,dd_due.date_sk as due_date_sk

        ,dim_salesreason.salesreason_sk as primary_salesreason_sk

        -- métricas
        ,int_sales.orderqty
        ,int_sales.unitprice
        ,int_sales.unitpricediscount
        ,int_sales.gross_revenue
        ,int_sales.discount_amount
        ,int_sales.net_revenue
        ,int_sales.taxamt
        ,int_sales.freight

        -- datas em formato nativo (facilita filtros no Power BI)
        ,int_sales.orderdate
        ,int_sales.shipdate
        ,int_sales.duedate

    from int_sales

    -- dimensões obrigatórias
    inner join dim_date              dd_order    on cast(int_sales.orderdate as date) = dd_order.full_date
    left  join dim_date              dd_ship     on cast(int_sales.shipdate  as date) = dd_ship.full_date
    left  join dim_date              dd_due      on cast(int_sales.duedate   as date) = dd_due.full_date
    left  join dim_customer                      on int_sales.customerid              = dim_customer.customerid
    left  join dim_product                       on int_sales.productid               = dim_product.productid
    left  join dim_creditcard                    on int_sales.creditcardid            = dim_creditcard.creditcardid
    left  join dim_status                        on int_sales.status                  = dim_status.statusid
    left  join dim_salesreason                   on int_sales.primary_salesreasonid   = dim_salesreason.salesreasonid

)

select * from final
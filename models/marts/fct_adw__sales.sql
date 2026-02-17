{{ config(
    materialized="table",
    schema="fct_adw"
) }}
with

int_sales as (
    select *
    from {{ ref('int_adw__sales') }}
),

dim_product as (
    select *
    from {{ ref('dim_adw__product') }}
),

dim_customer as (
    select *
    from {{ ref('dim_adw__customer') }}
),

dim_creditcard as (
    select *
    from {{ ref('dim_adw__creditcard') }}
),

dim_salesreason as (
    select *
    from {{ ref('dim_adw__salesreason') }}
),

dim_status as (
    select *
    from {{ ref('dim_adw__status') }}
),

dim_date as (
    select *
    from {{ ref('dim_adw__date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['salesorderid','salesorderdetailid']) }} as sales_sk
    ,salesorderid as pedido_pk

    ,dim_product.produto_sk
    ,dim_customer.cliente_sk
    ,dim_creditcard.cartao_credito_sk
    ,dim_salesreason.motivo_venda_sk
    ,dim_status.status_sk
    ,dim_date.data_sk

    ,(orderqty) as quantidade

    ,(orderqty * unitprice) as faturamento_bruto

    ,(orderqty * unitprice * unitpricediscount) as desconto

    ,(orderqty * unitprice) - (orderqty * unitprice * unitpricediscount) as valor_liquido

from int_sales
left join dim_product on int_sales.productid = dim_product.produto_pk
left join dim_customer on int_sales.customerid = dim_customer.cliente_pk
left join dim_creditcard on int_sales.creditcardid = dim_creditcard.cartao_credito_pk
left join dim_salesreason on int_sales.salesreasonid = dim_salesreason.motivo_venda_pk
left join dim_status on int_sales.status = dim_status.status_pk
left join dim_date on int_sales.orderdate = dim_date.data_pk
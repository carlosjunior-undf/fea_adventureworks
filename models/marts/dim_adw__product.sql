-- dim_adw__product.sql
-- Dimensão de produtos.
-- Granularidade: um registro por productid.
{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with int_product as (

    select * from {{ ref('int_adw__product') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['productid']) }}   as product_sk
        ,productid
        ,product_name
        ,listprice
        ,subcategory_name
        ,category_name

    from int_product

)

select * from final
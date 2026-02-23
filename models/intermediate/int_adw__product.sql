-- int_adw__product.sql
-- Camada intermediate: join entre product, subcategory e category para dimensão produto

{{ config(
    materialized="view",
    schema="int_adw"
) }}

with product as (

    select * from {{ ref('stg_adw__production_product') }}

),

subcategory as (

    select * from {{ ref('stg_adw__production_productsubcategory') }}

),

category as (

    select * from {{ ref('stg_adw__production_productcategory') }}

),

joined as (

    select
        product.productid
        ,product.name         as product_name
        ,product.color
        ,product.listprice

        ,subcategory.name     as subcategory_name

        ,category.name        as category_name

    from product              
    left join subcategory     on product.productsubcategoryid     = subcategory.productsubcategoryid
    left join category        on subcategory.productcategoryid    = category.productcategoryid

)

select * from joined
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
        p.productid
        ,p.name               as product_name
        ,p.color
        ,p.listprice
        ,sc.name              as subcategory_name
        ,c.name               as category_name

    from product              p
    left join subcategory     sc on p.productsubcategoryid  = sc.productsubcategoryid
    left join category        c  on sc.productcategoryid    = c.productcategoryid

)

select * from joined
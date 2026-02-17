{{ config(
    materialized="view",
    schema="int_adw"
) }}

with 

product as (

    select * 
    from {{ ref('stg_adw__production_product') }}

),

subcategory as (

    select *
    from {{ ref('stg_adw__production_productsubcategory') }}

),

category as (

    select *
    from {{ ref('stg_adw__production_productcategory') }}

)

select
    product.productid
    ,product.name as nome_produto
    ,product.color as cor_produto
    ,product.listprice as preco_lista
    ,subcategory.name as nome_subcategoria
    ,category.name as nome_categoria

from product 
left join subcategory on product.productsubcategoryid = subcategory.productsubcategoryid
left join category on subcategory.productcategoryid = category.productcategoryid

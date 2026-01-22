{{ config(
    materialized="view",
    schema="int_adw"
) }}
with
    production_product as (
        select *
        from {{ ref('stg_adw__production_product') }}
    ),
    production_productcategory as (
        select *
        from {{ ref('stg_adw__production_productcategory') }}
    ), 
    production_productsubcategory as (
        select *
        from {{ ref('stg_adw__production_productsubcategory') }}
    ),

    joined as (
        select
            production_product.produto_sk,
            production_product.nome_produto,

            production_productsubcategory.categoria_fk,

            production_productcategory.nome_categoria,
            
            production_product.subcategoria_fk,
            production_productsubcategory.nome_subcategoria,
            production_product.preco_lista
            --production_productcategory.categoria_pk,
            --production_productsubcategory.subcategoria_pk
        from production_product
        inner join production_productsubcategory on production_product.subcategoria_fk = production_productsubcategory.subcategoria_pk
        inner join production_productcategory on production_productsubcategory.categoria_fk = production_productcategory.categoria_pk
    )
select * from joined
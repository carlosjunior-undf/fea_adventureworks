with
    -- import CTES
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
    -- transformation
    joined as (
        select
            production.produto_sk,
            production.categoria_pk,
            production.subcategoria_pk,
            production.categoria_fk,
            production.product_fk,
            production.subcategoria_fk,
            production.nome_produto,
            production.preco_lista,
            production.nome_categoria,
            production.nome_subcategoria
        from production_product
        inner join production_productsubcategory on production.subcategoria_fk = production.subcategoria_pk
        inner join production_productsubcategory on production.categoria_fk =  production.categoria_pk

    )
select * from joined
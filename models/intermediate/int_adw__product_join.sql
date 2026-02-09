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
    production_location as (
        select *
        from {{ ref('stg_adw__production_location') }}
    ),
    production_productinventory as (
        select *
        from {{ ref('stg_adw__production_productinventory') }}
    ),

    joined as (
        select
        production_product.produto_pk
        ,production_product.subcategoria_fk
        ,production_product.nome_produto
        ,production_product.numero_produto
        ,production_product.cor_produto
        ,production_product.qtd_seguranca_estoque
        ,production_product.pto_abastecer_estoque
        ,production_product.custo_padrao
        ,production_product.preco_lista
        ,production_product.data_inicio_venda
        ,production_product.data_fim_venda
        ,production_product.data_completa

        ,production_productcategory.categoria_pk
        ,production_productcategory.nome_categoria
        --production_productcategory.data_completa

        ,production_productsubcategory.subcategoria_pk
        ,production_productsubcategory.categoria_fk
        ,production_productsubcategory.nome_subcategoria

        ,production_location.local_producao_pk
        ,production_location.nome_local_producao
        --production_location.data_completa

        ,production_productinventory.produto_fk
        ,production_productinventory.local_producao_fk
        ,production_productinventory.prateleira_produto
        ,production_productinventory.qtd_produzida
        --production_productinventory.data_completa

        from production_product
        inner join production_productinventory on production_product.produto_pk = production_productinventory.produto_fk
        inner join production_productsubcategory on production_product.subcategoria_fk = production_productsubcategory.subcategoria_pk
        inner join production_productcategory on production_productsubcategory.categoria_fk = production_productcategory.categoria_pk
        inner join production_location on production_productinventory.local_producao_fk = production_location.local_producao_pk
    )
select * from joined
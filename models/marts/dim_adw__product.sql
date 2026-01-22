{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_products as (
        select *
        from {{ ref('int_adw__product_join') }}
    ),
    
    dim_adw_products__metrics as (
        select
            produto_sk,
            nome_produto,
            nome_categoria,
            nome_subcategoria,
            preco_lista
        from int_products
    )
select * from dim_adw_products__metrics
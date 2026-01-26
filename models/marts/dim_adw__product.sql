{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_products as (
        select *
        from {{ ref('int_adw__product_join') }}
    ),
    
    product_transformed as (
        select
        -- Traga todas as colunas da int_adw__product_join, mas deixe apenas a SK e as demais colunas ativas.

            produto_sk
            ,numero_produto
            ,nome_produto
            ,cor_produto
            ,nome_categoria
            ,nome_subcategoria
            ,prateleira_produto
            ,nome_local_producao
            ,qtd_produzida
            ,qtd_seguranca_estoque
            ,pto_abastecer_estoque
            ,custo_padrao
            ,preco_lista
            ,data_inicio_venda
            ,data_fim_venda
            ,data_completa

        from int_products
    )
select * from product_transformed
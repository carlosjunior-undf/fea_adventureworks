{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with
    int_products as (
        select *
        from {{ ref('int_adw__product_join') }}
    ),
    
    product_transformed as (
        select
        {{ dbt_utils.generate_surrogate_key(['produto_pk']) }} as produto_sk
        ,produto_pk
        ,categoria_fk
        ,subcategoria_fk
        ,local_producao_fk
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
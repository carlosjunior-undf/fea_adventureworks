{{ config(
    materialized="table",
    schema="dim_adw"
) }}

with base as (

    select *
    from {{ ref('int_adw__product') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['productid']) }} as produto_sk
    ,productid as produto_pk
    ,nome_produto
    ,cor_produto
    ,preco_lista
    ,nome_subcategoria
    ,nome_categoria

from base

{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_production_product as (

    select * from {{ source('adw_production', 'production_product') }}
   
),

renamed as (

    select
        --{{ dbt_utils.generate_surrogate_key(['productid', 'productsubcategoryid']) }} as produto_sk,
        cast(productid as int) as produto_id,
        cast(productsubcategoryid as int) as subcategoria_id,
        cast(name as string) as nome_produto,
        cast(safetystocklevel as int) as qtd_seguranca_estoque,
        cast(reorderpoint as int) as pto_abastecer_estoque,
        cast(standardcost as float) as custo_padrao,
        --cast(color as string ) as cor_produto,
        cast (listprice as float) as preco_lista
        --productmodelid,
        --sellstartdate,
        --sellenddate,
        --discontinueddate,
        --rowguid,
        --cast(modifieddate as date) as modified_date
    from source_production_product

)

select * from renamed
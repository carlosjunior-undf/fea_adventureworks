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
        cast(productid as int) as produto_pk
        ,cast(productsubcategoryid as int) as subcategoria_fk
        ,cast(name as string) as nome_produto
        ,cast(productnumber as string) as numero_produto
        ,cast(color as string ) as cor_produto
        ,cast(safetystocklevel as int) as qtd_seguranca_estoque
        ,cast(reorderpoint as int) as pto_abastecer_estoque
        ,cast(standardcost as float) as custo_padrao
        ,cast(listprice as float) as preco_lista
        ,cast(sellstartdate as date) as data_inicio_venda
        ,cast(sellenddate as date) as data_fim_venda
        ,cast(modifieddate as date) as data_completa
        --makeflag,
        --finishedgoodsflag,
        --size,
        --sizeunitmeasurecode,
        --weightunitmeasurecode,
        --weight,
        --daystomanufacture,
        --productline,
        --class,
        --style,
        --discontinueddate,
        --rowguid,

    from source_production_product

)

select * from renamed
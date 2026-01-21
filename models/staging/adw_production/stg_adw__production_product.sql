with 

source_production_product as (

    select * from {{ source('adw_production', 'production_product') }}
   
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['productid', 'productsubcategoryid']) }} as produto_sk,
        cast(productid as int) as produto_fk,
        cast(productsubcategoryid as int) as subcategoria_fk,
        cast(name as string) as nome_produto,
        cast (listprice as float) as preco_lista
        --cast(modifieddate as date) as modified_date
        --productmodelid,
        --sellstartdate,
        --sellenddate,
        --discontinueddate,
        --rowguid,
    from source_production_product

)

select * from renamed
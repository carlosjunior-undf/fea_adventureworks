{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_production_productsubcategory as (

    select * from {{ source('adw_production', 'production_productsubcategory') }}

),

renamed as (

    select
        cast(productsubcategoryid as int) as subcategoria_id,
        cast(productcategoryid as int) as categoria_id,
        cast(name as string) as nome_subcategoria
        --cast(modifieddate as date) as modified_date
    from source_production_productsubcategory
)
select * from renamed
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
        cast(productsubcategoryid as int) as subcategoria_pk
        ,cast(productcategoryid as int) as categoria_fk
        ,cast(name as string) as nome_subcategoria
        ,cast(modifieddate as date) as data_completa
    from source_production_productsubcategory
)
select * from renamed
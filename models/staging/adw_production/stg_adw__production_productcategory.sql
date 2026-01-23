{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 
source_production_productcategory as (

    select * from {{ source('adw_production', 'production_productcategory') }}

),

renamed as (

    select
        cast(productcategoryid as int) as categoria_id,
        cast(name as string) as nome_categoria
        --cast(modifieddate as date) as modified_date
    from source_production_productcategory

)
select * from renamed
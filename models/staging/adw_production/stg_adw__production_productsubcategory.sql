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
        productsubcategoryid
        ,productcategoryid
        ,name
        ,rowguid
        ,modifieddate

    from source_production_productsubcategory

)

select * from renamed
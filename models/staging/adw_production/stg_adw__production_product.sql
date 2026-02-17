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
        productid
        ,name
        ,productnumber
        ,makeflag
        ,finishedgoodsflag
        ,color
        ,safetystocklevel
        ,reorderpoint
        ,standardcost
        ,listprice
        ,size
        ,sizeunitmeasurecode
        ,weightunitmeasurecode
        ,weight
        ,daystomanufacture
        ,productline
        ,class
        ,style
        ,productsubcategoryid
        ,productmodelid
        ,sellstartdate
        ,sellenddate
        ,discontinueddate
        ,rowguid
        ,modifieddate

    from source_production_product

)

select * from renamed
-- dim_adw__product.sql

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with int_product as (

    select * from {{ ref('int_adw__product2') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['productid']) }}   as product_sk
        ,productid
        ,product_name
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
        ,sellstartdate
        ,sellenddate
        ,discontinueddate
        ,productsubcategoryid
        ,subcategory_name
        ,productcategoryid
        ,category_name

    from int_product

)

select * from final
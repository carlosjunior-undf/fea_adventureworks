with 

source as (

    select * from {{ source('adw_production', 'production_productsubcategory') }}

),

renamed as (

    select
        productsubcategoryid,
        productcategoryid,
        name,
        rowguid,
        modifieddate

    from source

)

select * from renamed
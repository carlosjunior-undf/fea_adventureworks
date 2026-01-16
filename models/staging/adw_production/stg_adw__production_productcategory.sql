with 

source as (

    select * from {{ source('adw_production', 'production_productcategory') }}

),

renamed as (

    select
        productcategoryid,
        name,
        rowguid,
        modifieddate

    from source

)

select * from renamed
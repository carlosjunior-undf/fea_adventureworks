with 

source_sales_territory as (

    select * from {{ source('adw_sales', 'sales_territory') }}

),

renamed as (

    select
        territoryid,
        name,
        countryregioncode,
        group
    from source_sales_territory

)

select * from renamed
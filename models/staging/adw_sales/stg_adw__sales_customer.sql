with 

source as (

    select * from {{ source('adw_sales', 'sales_customer') }}

),

renamed as (

    select
        customerid,
        personid,
        storeid,
        territoryid,
        rowguid,
        modifieddate

    from source

)

select * from renamed
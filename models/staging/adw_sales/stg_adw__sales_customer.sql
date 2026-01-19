with 

source_sales_customer as (

    select * from {{ source('adw_sales', 'sales_customer') }}

),

renamed as (

    select
        cast(customerid as int) as customer_pk,
        cast(personid as float) as person_fk,
        cast(territoryid as int) as territory_fk,
        cast(modifieddate as date) as modified_date
    from source_sales_customer

)

select * from renamed
with 

source as (

    select * from {{ source('adw_person', 'person_emailaddress') }}

),

renamed as (

    select
        businessentityid,
        emailaddressid,
        emailaddress,
        rowguid,
        modifieddate

    from source

)

select * from renamed
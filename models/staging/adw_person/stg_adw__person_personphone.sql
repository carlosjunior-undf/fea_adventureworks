with 

source as (

    select * from {{ source('adw_person', 'person_personphone') }}

),

renamed as (

    select
        businessentityid,
        phonenumber,
        phonenumbertypeid,
        modifieddate

    from source

)

select * from renamed
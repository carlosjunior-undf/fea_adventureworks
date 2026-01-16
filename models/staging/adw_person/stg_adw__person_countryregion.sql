with 

source as (

    select * from {{ source('adw_person', 'person_countryregion') }}

),

renamed as (

    select
        countryregioncode,
        name,
        modifieddate

    from source

)

select * from renamed
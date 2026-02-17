{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_countryregion as (

    select * from {{ source('adw_person', 'person_countryregion') }}

),

renamed as (

    select
        countryregioncode
        ,name
        ,modifieddate

    from source_person_countryregion

)

select * from renamed
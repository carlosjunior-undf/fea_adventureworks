{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_stateprovince as (

    select * from {{ source('adw_person', 'person_stateprovince') }}

),

renamed as (

    select
        stateprovinceid
        ,stateprovincecode
        ,countryregioncode
        ,isonlystateprovinceflag
        ,name
        ,territoryid
        ,rowguid
        ,modifieddate

    from source_person_stateprovince

)

select * from renamed
{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_address as (

    select * from {{ source('adw_person', 'person_address') }}

),

renamed as (

    select
        addressid
        ,addressline1
        ,addressline2
        ,city
        ,stateprovinceid
        ,postalcode
        ,spatiallocation
        ,rowguid
        ,modifieddate

    from source_person_address

)

select * from renamed
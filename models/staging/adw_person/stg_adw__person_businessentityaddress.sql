{{ config(
    materialized="view",
    schema="stg_adw"
) }}

with 

source_entityaddress as (

    select * from {{ source('adw_person', 'person_businessentityaddress') }}

),

renamed as (

    select
        businessentityid
        ,addressid
        ,addresstypeid
        ,rowguid
        ,modifieddate

    from source_entityaddress

)

select * from renamed
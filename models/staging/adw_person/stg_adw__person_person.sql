{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_person as (

    select * from {{ source('adw_person', 'person_person') }}

),

renamed as (

    select
        businessentityid
        ,persontype
        ,namestyle
        ,title
        ,firstname
        ,middlename
        ,lastname
        ,suffix
        ,emailpromotion
        ,additionalcontactinfo
        ,demographics
        ,rowguid
        ,modifieddate

    from source_person_person

)

select * from renamed
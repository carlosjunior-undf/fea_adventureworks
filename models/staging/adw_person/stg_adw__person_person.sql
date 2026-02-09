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
        cast(businessentityid as int) as cliente_pk
        ,concat(firstname, ' ', lastname) as nome_pessoa
        ,cast(modifieddate as date) as data_completa
        --firstname,
        --middlename,
        --persontype,
        --namestyle,
        --title,
        --lastname,
        --suffix,
        --emailpromotion,
        --additionalcontactinfo,
        --demographics,
        --rowguid,
        
    from source_person_person
)

select * from renamed
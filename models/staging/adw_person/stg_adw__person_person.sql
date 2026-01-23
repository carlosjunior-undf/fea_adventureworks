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
        cast(businessentityid as int) as entidade_pessoa_id,
        concat(firstname, ' ', lastname) as nome_pessoa
        --cast(modifieddate as date) as data_completa
        --persontype,
        --namestyle,
        --title,
        --firstname,
        --middlename,
        --lastname,
        --suffix,
        --emailpromotion,
        --additionalcontactinfo,
        --demographics,
        --rowguid,
    from source_person_person
)

select * from renamed
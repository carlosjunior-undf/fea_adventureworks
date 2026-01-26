{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_emailaddress as (

    select * from {{ source('adw_person', 'person_emailaddress') }}

),

renamed as (

    select

        cast(emailaddressid as int) as email_pk
        ,cast(businessentityid as int) as entidade_pessoa_fk
        ,cast(emailaddress as string) as email_pessoa
        ,cast(modifieddate as date) as data_completa
        --rowguid

    from source_person_emailaddress
)
select * from renamed
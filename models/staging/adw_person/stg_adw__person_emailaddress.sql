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
        cast(emailaddressid as int) as email_id,
        cast(businessentityid as int) as entidade_pessoa_id,
        cast(emailaddress as string) as email_pessoa
        --rowguid,
        --cast(modifieddate as date) as modified_date
    from source_person_emailaddress
)
select * from renamed
{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_personphone as (

    select * from {{ source('adw_person', 'person_personphone') }}

),

renamed as (

    select

        cast(businessentityid as int) as cliente_fk
        ,cast(phonenumber as string) as telefone_pessoa
        ,cast(modifieddate as date) as data_completa
        --phonenumbertypeid,

    from source_person_personphone
)
select * from renamed
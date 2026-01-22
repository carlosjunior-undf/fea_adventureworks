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
        cast(businessentityid as int) as entidade_empresa_fk,
        cast(phonenumber as string) as numero_telefone
        --cast(modifieddate as date) as modified_date
        --phonenumbertypeid,
    from source_person_personphone
)
select * from renamed
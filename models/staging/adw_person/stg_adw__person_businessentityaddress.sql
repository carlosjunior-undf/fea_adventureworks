{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_businessentityaddress as (

    select * from {{ source('adw_person', 'person_businessentityaddress') }}

),

renamed as (

    select
        --{{ dbt_utils.generate_surrogate_key(['businessentityid','addressid']) }} as localidade_sk,
        cast(businessentityid as int) as entidade_pessoa_id,
        cast(addressid as int) as endereco_id,
        --cast(modifieddate as date) as data_completa
        --addresstypeid,
        --rowguid
    from source_person_businessentityaddress

)

select * from renamed
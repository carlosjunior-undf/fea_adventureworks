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
        cast(addressid as int) as endereco_id,
        cast(stateprovinceid as int) as estado_id,
        cast(addressline1 as string) as endereco_pessoa,
        cast(postalcode as string) as cep_pessoa,
        cast(city as string) as cidade_pessoa
        --cast(modifieddate as date) as data_completa
        --spatiallocation,
        --rowguid,
    from source_person_address

)
select * from renamed
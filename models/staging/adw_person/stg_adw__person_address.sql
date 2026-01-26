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

        {{ dbt_utils.generate_surrogate_key(['addressid', 'stateprovinceid']) }} as endereco_sk
        ,cast(addressid as int) as endereco_pk
        ,cast(stateprovinceid as int) as estado_fk
        ,cast(addressline1 as string) as endereco_pessoa
        ,cast(postalcode as string) as cep_pessoa
        ,cast(city as string) as cidade_pessoa
        ,cast(modifieddate as date) as data_completa
        --spatiallocation,
        --rowguid,
    
    from source_person_address

)
select * from renamed
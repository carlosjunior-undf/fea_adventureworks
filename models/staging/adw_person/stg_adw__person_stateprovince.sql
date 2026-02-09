{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_stateprovince as (

    select * from {{ source('adw_person', 'person_stateprovince') }}

),

renamed as (

    select
        cast(stateprovinceid as int) as estado_pk
        ,cast(territoryid as int) as territorio_fk
        ,cast(stateprovincecode as string) as codigo_estado
        ,cast(name as string) as nome_estado
        ,cast(countryregioncode as string) as codigo_pais_fk
        ,cast(modifieddate as date) as data_completa
        --isonlystateprovinceflag,
        --rowguid,

    from source_person_stateprovince
)
select * from renamed
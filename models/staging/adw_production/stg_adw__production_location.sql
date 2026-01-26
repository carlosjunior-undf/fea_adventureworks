{{ config(
    materialized="view",
    schema="stg_adw"
) }}

with 

source_production_location as (

    select * from {{ source('adw_production', 'production_location') }}

),

renamed as (

    select
        cast(locationid as int) as local_producao_pk
        ,cast(name as string) as nome_local_producao
        ,cast(modifieddate as date) as data_completa
        --costrate,
        --availability,

    from source_production_location

)

select * from renamed
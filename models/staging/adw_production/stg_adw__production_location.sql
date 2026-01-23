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
        cast(locationid as int) as local_producao_id,
        cast(name as string) as nome_local_producao
        --costrate,
        --availability,
        --modifieddate

    from source_production_location

)

select * from renamed
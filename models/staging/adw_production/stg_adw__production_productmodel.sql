{{ config(
    materialized="view",
    schema="stg_adw"
) }}

with 

source_production_productmodel as (

    select * from {{ source('adw_production', 'production_productmodel') }}

),

renamed as (

    select
        cast(productmodelid as int) as modelo_produto_id,
        cast(name as string) as nome_modelo_produto
        --catalogdescription,
        --instructions,
        --rowguid,
        --modifieddate

    from source_production_productmodel

)

select * from renamed
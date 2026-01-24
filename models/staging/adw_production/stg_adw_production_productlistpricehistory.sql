{{ config(
    materialized="view",
    schema="stg_adw"
) }}

with 

source_production_productlistpricehistory as (

    select * from {{ source('adw_production', 'production_productlistpricehistory') }}

),

renamed as (

    select
        cast(productid as int) as produto_id,
        cast(startdate as date) as data_inicio,
        cast(enddate as date) as data_fim,
        cast(listprice as float) as preco_lista
        --modifieddate

    from source_production_productlistpricehistory

)

select * from renamed
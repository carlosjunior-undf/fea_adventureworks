{{ config(
    materialized="view",
    schema="stg_adw"
) }}

with 
source_production_productcosthistory as (

    select * from {{ source('adw_production', 'production_productcosthistory') }}

),

renamed as (

    select
        cast(productid as int) as produto_id,
        cast(startdate as date) as data_inicio,
        cast(enddate as date) as data_fim,
        cast(standardcost as float) as custo_padrao
        --modifieddate

    from source_production_productcosthistory

)

select * from renamed
{{ config(
    materialized="view",
    schema="stg_adw"
) }}

with 

source_production_productinventory as (

    select * from {{ source('adw_production', 'production_productinventory') }}

),

renamed as (

    select
        cast(productid as int) as produto_fk
        ,cast(locationid as int) as local_producao_fk
        ,cast(shelf as string) as prateleira_produto
        ,cast(quantity as int) as qtd_produzida
        ,cast(modifieddate as date) as data_completa
        --bin,
        --rowguid,
    from source_production_productinventory

)

select * from renamed
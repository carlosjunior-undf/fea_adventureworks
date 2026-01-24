{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 
source_sales_customer as (
    select * from {{ source('adw_sales', 'sales_customer') }}
),

renamed as (

    select
        cast(customerid as int) as cliente_id,
        cast(personid as float) as pessoa_id,
        cast(territoryid as int) as territorio_id
        --storeid
        --rowguid
        --cast(modifieddate as date) as data_completa
    from source_sales_customer

)
select * from renamed
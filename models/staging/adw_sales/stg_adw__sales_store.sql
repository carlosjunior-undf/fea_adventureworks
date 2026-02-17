{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_store as (

    select * from {{ source('adw_sales', 'sales_store') }}

),

renamed as (

    select
        businessentityid
        ,name
        ,salespersonid
        ,demographics
        ,rowguid
        ,modifieddate

    from source_sales_store

)

select * from renamed
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
        {{ dbt_utils.generate_surrogate_key(['customerid', 'personid', 'territoryid']) }} as cliente_sk
        ,cast(customerid as int) as cliente_pk
        ,cast(personid as int) as pessoa_fk
        ,cast(territoryid as int) as territorio_fk
        ,cast(modifieddate as date) as data_completa
--        ,storeid
--        ,rowguid
    from source_sales_customer

)

select * from renamed
{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_creditcard as (

    select * from {{ source('adw_sales', 'sales_creditcard') }}

),

renamed as (

    select
        creditcardid
        ,cardtype
        ,cardnumber
        ,expmonth
        ,expyear
        ,modifieddate

    from source_sales_creditcard

)

select * from renamed
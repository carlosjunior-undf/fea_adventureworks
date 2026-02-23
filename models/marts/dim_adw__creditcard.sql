-- dim_adw__creditcard.sql

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with source as (

    select * from {{ ref('stg_adw__sales_creditcard') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as creditcard_sk
        ,creditcardid
        ,cardtype

    from source

)

select * from final
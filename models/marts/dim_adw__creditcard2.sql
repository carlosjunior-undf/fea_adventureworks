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
--        mascara o número por segurança: mantém apenas últimos 4 dígitos
        ,concat('****-****-****-', right(cast(cardnumber as string), 4)) as cardnumber_masked
        ,expmonth
        ,expyear
        ,make_date(expyear, expmonth, 1) as expiration_date

    from source

)

select * from final
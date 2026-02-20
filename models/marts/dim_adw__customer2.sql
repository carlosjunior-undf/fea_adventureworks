-- dim_adw__customer.sql

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with int_customer as (

    select * from {{ ref('int_adw__customer2') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customer_sk
        ,customerid
        ,addressid
        ,stateprovinceid
        ,personid
        ,storeid
        ,territoryid
        ,persontype
        ,title
        ,firstname
        ,middlename
        ,lastname

        ,concat(firstname, '', ' ', lastname, '' ) as full_name

        ,emailpromotion
        ,addressline1
        ,addressline2
        ,city
        ,postalcode
        ,stateprovincecode
        ,state_name
        ,countryregioncode
        ,country_name

    from int_customer

)

select * from final
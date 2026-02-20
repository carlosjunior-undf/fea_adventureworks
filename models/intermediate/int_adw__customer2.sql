{{ config(
    materialized="view",
    schema="int_adw"
) }}
-- int_adw__customer.sql
-- Camada intermediate: join entre customer, person e address para dimensão customer

with customer as (

    select * from {{ ref('stg_adw__sales_customer') }}

),

person as (

    select * from {{ ref('stg_adw__person_person') }}

),

entity_address as (

    select * from {{ ref('stg_adw__person_businessentityaddress') }}

),

address as (

    select * from {{ ref('stg_adw__person_address') }}

),

state_province as (

    select * from {{ ref('stg_adw__person_stateprovince') }}

),

country_region as (

    select * from {{ ref('stg_adw__person_countryregion') }}

),

joined as (

    select
        customer.customerid
        ,customer.personid
        ,customer.storeid
        ,customer.territoryid

        ,person.persontype
        ,person.title
        ,person.firstname
        ,person.middlename
        ,person.lastname
        ,person.emailpromotion
        -- endereço: usa o primeiro endereço vinculado ao businessentity do customer
        ,address.addressid
        ,address.addressline1
        ,address.addressline2
        ,address.city
        ,address.postalcode

        ,state_province.stateprovinceid
        ,state_province.stateprovincecode
        ,(state_province.name) as state_name

        ,country_region.countryregioncode
        ,(country_region.name) as country_name

    from customer
    left join person on customer.personid = person.businessentityid
    -- pega apenas o primeiro addresstypeid por businessentity para evitar fan-out
    left join (
        select
            businessentityid
            ,addressid
            ,row_number() over (partition by businessentityid order by addresstypeid) as rn
        from entity_address
    )                               entity_address on customer.personid = entity_address.businessentityid
                                       and entity_address.rn = 1
    left join address on entity_address.addressid = address.addressid
    left join state_province on address.stateprovinceid = state_province.stateprovinceid
    left join country_region on state_province.countryregioncode = country_region.countryregioncode

)

select * from joined
-- int_adw__customer.sql
-- Camada intermediate: join entre customer, person e address.

{{ config(
    materialized="view",
    schema="int_adw"
) }}

with customer as (

    select * from {{ ref('stg_adw__sales_customer') }}

),

person as (

    select * from {{ ref('stg_adw__person_person') }}

),

business_entity_address as (

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
        c.customerid
        ,p.firstname
        ,p.middlename
        ,p.lastname
        ,a.city
        ,sp.name    as state_name
        ,cr.name    as country_name

    from customer                   c
    left join person                p   on c.personid            = p.businessentityid
    -- pega apenas o primeiro endereço vinculado ao businessentity para evitar fan-out
    left join (
        select
            businessentityid
            ,addressid
            ,row_number() over (partition by businessentityid order by addresstypeid) as rn
        from business_entity_address
    )                               bea on c.personid            = bea.businessentityid
                                       and bea.rn                = 1
    left join address               a   on bea.addressid         = a.addressid
    left join state_province        sp  on a.stateprovinceid     = sp.stateprovinceid
    left join country_region        cr  on sp.countryregioncode  = cr.countryregioncode

)

select * from joined

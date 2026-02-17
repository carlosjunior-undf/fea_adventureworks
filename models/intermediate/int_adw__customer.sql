{{ config(
    materialized="view",
    schema="int_adw"
) }}

with 

customer as (

    select *
    from {{ ref('stg_adw__sales_customer') }}

),

person as (

    select *
    from {{ ref('stg_adw__person_person') }}

),

entity_address as (

    select *
    from {{ ref('stg_adw__person_businessentityaddress') }}

),

address as (

    select *
    from {{ ref('stg_adw__person_address') }}

),

state as (

    select *
    from {{ ref('stg_adw__person_stateprovince') }}

),

country as (

    select *
    from {{ ref('stg_adw__person_countryregion') }}

), 

store as (
    select *
    from {{ ref('stg_adw__sales_store') }}
),

joined as (

    select
        customer.customerid

        ,coalesce(
            concat(person.firstname,' ',person.lastname)
            ,store.name
        ) as nome_cliente

        ,address.city as cidade
        ,state.name as estado
        ,country.name as pais

        ,row_number() over (
            partition by customer.customerid
            order by address.addressid
        ) as rn

from customer
left join person on customer.personid = person.businessentityid
left join store on customer.storeid = store.businessentityid
left join entity_address on coalesce(person.businessentityid, store.businessentityid) = entity_address.businessentityid
left join address on entity_address.addressid = address.addressid
left join state on address.stateprovinceid = state.stateprovinceid
left join country on state.countryregioncode = country.countryregioncode

)
select * from joined
where rn = 1
-- dim_adw__customer.sql
-- Dimensão de clientes.
-- Granularidade: um registro por customerid.

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with int_customer as (

    select * from {{ ref('int_adw__customer') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customerid']) }}  as customer_sk
        ,customerid
        ,concat((firstname) ,' ' ,(lastname)) as full_name
        ,city
        ,state_name
        ,country_name

    from int_customer

)

select * from final

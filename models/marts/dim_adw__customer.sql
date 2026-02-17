{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with 

transformed_customer as (

    select *
    from {{ ref('int_adw__customer') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} as cliente_sk
    ,customerid as cliente_pk
    ,nome_cliente
    ,cidade
    ,estado
    ,pais

from transformed_customer

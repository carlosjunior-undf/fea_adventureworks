{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with 
dates as (

    select distinct
        orderdate as data
    from {{ source('adw_sales','sales_salesorderheader') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['data']) }} as data_sk
        ,data as data_pk
        ,year(data) as ano
        ,month(data) as mes
        ,date_format(data, 'MMMM') as nome_mes
        ,day(data) as dia
        ,concat(year(data), '-', lpad(month(data),2,'0')) as ano_mes
from dates
)

select * from final

{{ config(
    materialized="view",
    schema="dim_adw"
) }}

with 

source_dates as (

    select
        cast(modifieddate as date) as data_completa
    from {{ source('adw_sales', 'sales_salesorderheader') }}

),

distinct_dates as (

    select distinct
        data_completa
    from source_dates
    where data_completa is not null

),

calendar_attributes as (

    select
        data_completa,
        day(data_completa)        as dia,
        month(data_completa)      as mes,
        monthname(data_completa) as nome_mes,
        year(data_completa)       as ano,
        weekofyear(data_completa) as semana_do_ano,
        date_format(data_completa, 'EEEE') as nome_dia,
        quarter(data_completa)    as trimestre,
        CASE WHEN dayofweek(data_completa) IN (1, 7) THEN TRUE ELSE FALSE END AS final_de_semana
    from distinct_dates

),

surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['data_completa']) }} as data_sk,
        data_completa,
        dia,
        mes,
        ano,
        nome_dia,
        final_de_semana,
        nome_mes,
        trimestre,
        semana_do_ano
    from calendar_attributes

)

select *from surrogate_key

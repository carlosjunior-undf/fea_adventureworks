-- dim_adw__date.sql (Tabela de Dimensão de Data)

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart   = "day",
            start_date = "cast('2010-01-01' as date)",
            end_date   = "cast('2030-12-31' as date)"
        )
    }}

),

final as (

   select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }}    as date_sk
        ,cast(date_day as date)                                 as full_date
        ,year(date_day)                                         as year
        ,quarter(date_day)                                      as quarter
        ,month(date_day)                                        as month_number
        ,date_format(date_day, 'MMMM')                          as month_name
        ,date_format(date_day, 'yyyy-MM')                       as year_month   -- ex: 2011-01

    from date_spine

)

select * from final

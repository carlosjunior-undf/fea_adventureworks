-- dim_adw__date.sql

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart   = "day",
            start_date = "cast('2000-01-01' as date)",
            end_date   = "cast('2030-12-31' as date)"
        )
    }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk
        ,cast(date_day as date)                                  as full_date
        ,cast(date_format(date_day, 'yyyyMMdd') as int)          as date_id          -- ex: 20110101
        ,year(date_day)                                          as year
        ,quarter(date_day)                                       as quarter
        ,month(date_day)                                         as month_number
        ,date_format(date_day, 'MMMM')                          as month_name
        ,date_format(date_day, 'MMM')                           as month_short
        ,weekofyear(date_day)                                    as week_of_year
        ,dayofweek(date_day)                                     as day_of_week       -- 1=Sunday
        ,date_format(date_day, 'EEEE')                          as day_name
        ,day(date_day)                                           as day_of_month
        ,dayofyear(date_day)                                     as day_of_year
        ,date_format(date_day, 'yyyy-MM')                       as year_month        -- ex: 2011-01
        ,concat('Q', quarter(date_day), ' ', year(date_day))    as year_quarter      -- ex: Q1 2011
        ,case when dayofweek(date_day) in (1, 7)
              then false else true end                           as is_weekday
              
        -- início / fim de período (útil para filtros em Power BI)
        ,date_trunc('month', date_day)                           as first_day_of_month
        ,last_day(date_day)                                      as last_day_of_month
        ,date_trunc('year', date_day)                            as first_day_of_year

    from date_spine

)

select * from final
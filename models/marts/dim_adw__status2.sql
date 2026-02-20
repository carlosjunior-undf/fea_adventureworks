-- dim_adw__status.sql
-- Dimensão estática derivada do domínio do campo status do salesorderheader
-- 1=In process | 2=Approved | 3=Backordered | 4=Rejected | 5=Shipped | 6=Cancelled

{{ config(
    materialized = "table",
    schema       = "marts_adw"
) }}

with status_values as (

    select 1 as statusid, 'In Process'   as status_name, 'Open'   as status_group union all
    select 2,             'Approved',                     'Open'                    union all
    select 3,             'Backordered',                  'Open'                    union all
    select 4,             'Rejected',                     'Closed'                  union all
    select 5,             'Shipped',                      'Closed'                  union all
    select 6,             'Cancelled',                    'Closed'

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['statusid']) }}    as status_sk
        ,statusid
        ,status_name
        ,status_group

    from status_values

)

select * from final

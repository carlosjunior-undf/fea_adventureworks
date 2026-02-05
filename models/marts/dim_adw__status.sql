{{config(
    materialized="table",
    schema="dim_adw"
)}}

with
source_status as (
    select
        status
    from {{ source('adw_sales', 'sales_salesorderheader') }}
),
    
distinct_status as (

    select distinct
        status
    from source_status
    where status is not null

),

status_descriptios as (
    select

        status
        ,case
            when status = 1 then 'Em processamento'
            when status = 2 then 'Aprovado'
            when status = 3 then 'Em espera'
            when status = 4 then 'Rejeitado'
            when status = 5 then 'Enviado'
            else 'Desconhecido'
        end as descricao_status

    from distinct_status
),

status_trasformed as (

    select

        {{ dbt_utils.generate_surrogate_key(['status']) }} as status_sk
        ,cast(status as int) as codigo_status
        ,descricao_status

    from status_descriptios

)
select * from status_trasformed
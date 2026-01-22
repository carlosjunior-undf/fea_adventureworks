{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_salesterritory as (

    select * from {{ source('adw_sales', 'sales_salesterritory') }}

),

renamed as (

    select
        cast(territoryid as int) as territorio_pk,
        cast(name as string) as nome_territorio,
        cast(countryregioncode as string) as codigo_pais,
        cast(group as string) as grupo_territorio
        --salesytd,
        --saleslastyear,
        --costytd,
        --costlastyear,
        --rowguid,
        --cast(modifieddate as data) as modified_date
    from source_sales_salesterritory

)
select * from renamed
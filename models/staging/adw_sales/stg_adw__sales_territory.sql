with 

source_sales_territory as (

    select * from {{ source('adw_sales', 'sales_salesterritory') }}

),

renamed as (

    select
        cast(territoryid as int) as territorio_pk,
        cast(name as string) as nome_territorio,
        cast(countryregioncode as string) as codigo_regiao_pais,
        cast(group as string) as grupo_territorio
    from source_sales_territory

)

select * from renamed
with 

source_person_stateprovince as (

    select * from {{ source('adw_person', 'person_stateprovince') }}

),

renamed as (

    select
        cast(stateprovinceid as int) as estado_pk,
        cast(stateprovincecode as string) as codigo_estado,
        cast(countryregioncode as string) as codigo_pais_pk,
        --isonlystateprovinceflag,
        cast(name as string) as nome_estado,
        cast(territoryid as int) as territorio_fk,
        --rowguid,
        cast(modifieddate as date) as modified_date
    from source_person_stateprovince
)

select * from renamed
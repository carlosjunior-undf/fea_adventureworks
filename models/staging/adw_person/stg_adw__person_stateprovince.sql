with 

source_person_stateprovince as (

    select * from {{ source('adw_person', 'person_stateprovince') }}

),

renamed as (

    select
        cast(stateprovinceid as int) as estado_pk,
        cast(stateprovincecode as string) as codigo_estado,
        cast(countryregioncode as string) as codigo_regiao_pais,
        --isonlystateprovinceflag,
        cast(name as string) as nome_regiao,
        cast(territoryid as int) as territorio_fk,
        rowguid,
        modifieddate

    from source_person_stateprovince

)

select * from renamed
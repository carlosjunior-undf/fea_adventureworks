with 

source_person_countryregion as (

    select * from {{ source('adw_person', 'person_countryregion') }}

),

renamed as (

    select
        cast(countryregioncode as string) as codigo_pais_fk,
        cast(name as string) as nome_pais
        --cast(modifieddate as date) as modified_date
    from source_person_countryregion
)
select * from renamed
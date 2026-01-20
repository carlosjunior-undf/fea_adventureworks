with 

source_person_businessentity as (

    select * from {{ source('adw_person', 'person_businessentity') }}

),

renamed as (

    select
        cast(businessentityid as int) as entidade_empresa_pk,
        --rowguid,
        cast(modifieddate as date) as modified_date
    from source_person_businessentity

)
select * from renamed
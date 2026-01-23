{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_businessentity as (

    select * from {{ source('adw_person', 'person_businessentity') }}

),

renamed as (

    select
        
        cast(businessentityid as int) as entidade_pessoa_id,
        --cast(modifieddate as date) as modified_date
        --rowguid,
    from source_person_businessentity

)
select * from renamed
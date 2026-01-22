{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_person_person as (

    select * from {{ source('adw_person', 'person_person') }}

),

renamed as (

    select
        cast(businessentityid as int) as entidade_empresa_fk,
        concat(firstname, ' ', lastname) as nome_cliente
        --cast(modifieddate as date) as modified_date
        --persontype,
        --namestyle,
        --title,
        --firstname,
        --middlename,
        --lastname,
        --suffix,
        --emailpromotion,
        --additionalcontactinfo,
        --demographics,
        --rowguid,
    from source_person_person
)

select * from renamed
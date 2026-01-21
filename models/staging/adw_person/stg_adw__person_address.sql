with 

source_person_address as (

    select * from {{ source('adw_person', 'person_address') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['addressid']) }} as localidade_sk,
        cast(addressid as int) as endereco_pk,
        cast(addressline1 as string) as endereco_pessoa,
        --addressline2,
        cast(city as string) as cidade_pessoa,
        cast(stateprovinceid as int) as estado_fk
        --postalcode,
        --spatiallocation,
        --rowguid,
        --cast(modifieddate as date) as modified_date
    from source_person_address

)
select * from renamed
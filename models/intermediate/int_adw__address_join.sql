{{ config(
    materialized="view",
    schema="int_adw"
) }}
with
    person_address as (
        select *
        from {{ ref('stg_adw__person_address') }}
    ),

    person_businessentityaddress as (
        select *
        from {{ ref('stg_adw__person_businessentityaddress') }}
    ), 
    person_countryregion as (
        select *
        from {{ ref('stg_adw__person_countryregion') }}
    ),
    person_stateprovince as (
        select *
        from {{ ref('stg_adw__person_stateprovince') }}
    ),

    joined as (
        select

            person_address.estado_id,
            person_address.endereco_id,
            person_address.endereco_pessoa,
            person_address.cep_pessoa,
            person_address.cidade_pessoa,

            person_businessentityaddress.entidade_pessoa_id,
            person_businessentityaddress.endereco_id,

            person_stateprovince.estado_id,
            person_stateprovince.territorio_id,
            person_stateprovince.codigo_estado,
            person_stateprovince.nome_estado,
            person_stateprovince.codigo_pais,
           

            person_countryregion.codigo_pais,
            person_countryregion.nome_pais

        from person_address
        inner join person_businessentityaddress on person_address.endereco_id = person_businessentityaddress.endereco_id
        inner join person_stateprovince on person_address.estado_id = person_stateprovince.estado_id
        inner join person_emailaddress on person_businessentityaddress.entidade_pessoa_id = person_emailaddress.entidade_pessoa_id
        inner join person_person on person_businessentityaddress.entidade_pessoa_id = person_person.entidade_pessoa_id
        inner join person_personphone on person_businessentityaddress.entidade_pessoa_id = person_personphone.entidade_pessoa_id
        inner join person_countryregion on person_stateprovince.codigo_pais = person_countryregion.codigo_pais


    )
select * from joined
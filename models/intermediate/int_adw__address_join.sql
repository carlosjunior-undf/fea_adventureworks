{{ config(
    materialized="view",
    schema="int_adw"
) }}
with

    person_businessentityaddress as (
        select *
        from {{ ref('stg_adw__person_businessentityaddress') }}
    ), 
    person_address as (
        select *
        from {{ ref('stg_adw__person_address') }}
    ),
    person_stateprovince as (
        select *
        from {{ ref('stg_adw__person_stateprovince') }}
    ),
    person_countryregion as (
        select *
        from {{ ref('stg_adw__person_countryregion') }}
    ),

    joined as (
        select
            person_businessentityaddress.localidade_sk,
            person_businessentityaddress.endereco_fk,

            person_address.endereco_pk,
            person_address.endereco_pessoa,
            person_address.cep_pessoa,
            person_address.cidade_pessoa,
            person_address.estado_fk,

            person_stateprovince.estado_pk,
            person_stateprovince.codigo_estado,
            person_stateprovince.nome_estado,
--            person_stateprovince.codigo_pais,
            person_stateprovince.territorio_fk,

            person_countryregion.codigo_pais,
            person_countryregion.nome_pais
        from person_address
        inner join person_businessentityaddress on person_address.endereco_pk = person_businessentityaddress.endereco_fk
        inner join person_stateprovince on person_address.estado_fk = person_stateprovince.estado_pk
        inner join person_countryregion on person_stateprovince.codigo_pais = person_countryregion.codigo_pais

    )
select * from joined
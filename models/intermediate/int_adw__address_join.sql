{{ config(
    materialized="view",
    schema="int_adw"
) }}
with
    -- import CTES
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
-- transformation
    joined as (
        select
            person_address.localidade_sk,
            --person_address.estado_fk,
            person_address.cidade_pessoa,

            person_stateprovince.estado_pk,
            person_stateprovince.codigo_estado,
            person_stateprovince.nome_estado,
            person_stateprovince.codigo_pais,
            person_countryregion.nome_pais
            --person_countryregion.codigo_pais_fk,
        from person_address
        inner join person_stateprovince on person_address.estado_fk = person_stateprovince.estado_pk
        inner join person_countryregion on person_stateprovince.codigo_pais = person_countryregion.codigo_pais
    )
select * from joined
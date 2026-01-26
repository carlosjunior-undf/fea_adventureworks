{{ config(
    materialized="view",
    schema="int_adw"
) }}
with

    person_address as (
        select *
        from {{ ref('stg_adw__person_address') }}
    ), 
    person_countryregion as (
        select *
        from {{ ref('stg_adw__person_countryregion') }}
    ),
    person_stateprovince as (
        select *
        from {{ ref('stg_adw__person_stateprovince') }}
    ),
    sales_salesterritory as (
        select *
        from {{ ref('stg_adw__sales_salesterritory') }}
    ),

    joined as (
        select

        person_address.endereco_sk
        ,person_address.endereco_pk
        ,person_address.estado_fk
        ,person_address.endereco_pessoa
        ,person_address.cep_pessoa
        ,person_address.cidade_pessoa
        ,person_address.data_completa

        ,person_stateprovince.estado_pk
        ,person_stateprovince.territorio_fk
        ,person_stateprovince.codigo_estado
        ,person_stateprovince.nome_estado
        ,person_stateprovince.codigo_pais_fk
--        ,person_stateprovince.data_completa

        ,person_countryregion.codigo_pais_pk
        ,person_countryregion.nome_pais
--        ,person_countryregion.data_completa

        ,sales_salesterritory.territorio_pk
        ,sales_salesterritory.nome_territorio
        ,sales_salesterritory.codigo_pais
        ,sales_salesterritory.grupo_territorio
--        ,sales_salesterritory.data_completa

        from person_stateprovince
        inner join person_address on person_stateprovince.estado_pk = person_address.estado_fk
        inner join sales_salesterritory on person_stateprovince.territorio_fk = sales_salesterritory.territorio_pk
        inner join person_countryregion on person_stateprovince.codigo_pais_fk = person_countryregion.codigo_pais_pk


    )
select * from joined
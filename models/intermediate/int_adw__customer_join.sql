{{ config(
    materialized="view",
    schema="int_adw"
) }}

with
    person_person as (
        select *
        from {{ ref('stg_adw__person_person') }}
    ),
    person_address as (
        select *
        from {{ ref('stg_adw__person_address') }}
    ), 
    person_businessentityaddress as (
        select *
        from {{ ref('stg_adw__person_businessentityaddress') }}
    ), 
    person_emailaddress as (
        select *
        from {{ ref('stg_adw__person_emailaddress') }}
    ),
    person_personphone as (
        select *
        from {{ ref('stg_adw__person_personphone') }}
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

    -- Após realizar dos joins, copie todas as colunas para a tabela de dimensão correspondente.
    
    joined as (
        select

        person_businessentityaddress.cliente_sk
        ,person_businessentityaddress.cliente_fk
        ,person_businessentityaddress.endereco_fk

        ,person_person.cliente_pk
        ,person_person.nome_pessoa

        ,person_address.endereco_pk
        ,person_address.estado_fk
        ,person_address.endereco_pessoa
        ,person_address.cep_pessoa
        ,person_address.cidade_pessoa

        ,person_emailaddress.email_pk
        ,person_emailaddress.email_pessoa

        ,person_personphone.telefone_pessoa
        ,person_personphone.data_completa

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


        from person_person
        inner join person_businessentityaddress on person_person.cliente_pk = person_businessentityaddress.cliente_fk
        inner join person_address on person_businessentityaddress.endereco_fk = person_address.endereco_pk
        inner join person_emailaddress on person_person.cliente_pk = person_emailaddress.cliente_fk
        inner join person_personphone on person_person.cliente_pk = person_personphone.cliente_fk
        inner join person_stateprovince on person_address.estado_fk = person_stateprovince.estado_pk
        inner join sales_salesterritory on person_stateprovince.territorio_fk = sales_salesterritory.territorio_pk
        inner join person_countryregion on person_stateprovince.codigo_pais_fk = person_countryregion.codigo_pais_pk

    )
select * from joined
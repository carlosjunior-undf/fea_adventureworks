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





    -- Após realizar dos joins, copie todas as colunas para a tabela de dimensão correspondente.
    
    joined as (
        select

        person_person.cliente_sk
        ,person_person.cliente_pk
        ,person_person.nome_pessoa
--        ,person_person.data_completa

        ,person_address.endereco_pk
        ,person_address.estado_fk
        ,person_address.endereco_pessoa
        ,person_address.cep_pessoa
        ,person_address.cidade_pessoa
--        ,person_address.data_completa

        ,person_businessentityaddress.cliente_fk
        ,person_businessentityaddress.endereco_fk
--        person_businessentityaddress.data_completa

        ,person_emailaddress.email_pk
--        ,person_emailaddress.cliente_fk
        ,person_emailaddress.email_pessoa
--        ,person_emailaddress.data_completa

--        ,person_personphone.cliente_fk
        ,person_personphone.telefone_pessoa
        ,person_personphone.data_completa

        from person_person
        inner join person_businessentityaddress on person_person.cliente_pk = person_businessentityaddress.cliente_fk
        inner join person_address on person_businessentityaddress.endereco_fk = person_address.endereco_pk
        inner join person_emailaddress on person_person.cliente_pk = person_emailaddress.cliente_fk
        inner join person_personphone on person_person.cliente_pk = person_personphone.cliente_fk

    )
select * from joined
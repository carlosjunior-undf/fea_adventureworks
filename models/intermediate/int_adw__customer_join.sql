{{ config(
    materialized="view",
    schema="int_adw"
) }}

with
    person_person as (
        select *
        from {{ ref('stg_adw__person_person') }}
    ), 
    person_emailaddress as (
        select *
        from {{ ref('stg_adw__person_emailaddress') }}
    ),
    person_personphone as (
        select *
        from {{ ref('stg_adw__person_personphone') }}
    ),

    joined as (
        select

        person_person.entidade_empresa_fk,
        person_person.nome_cliente,

        person_emailaddress.email_pk,
--        person_emailaddress.entidade_empresa_fk,
        person_emailaddress.email_pessoa,

--        person_personphone.entidade_empresa_fk,
        person_personphone.numero_telefone
        from person_emailaddress
        inner join person_person on person_emailaddress.entidade_empresa_fk = person_person.entidade_empresa_fk
        inner join person_personphone on person_emailaddress.entidade_empresa_fk = person_personphone.entidade_empresa_fk
    )
select * from joined
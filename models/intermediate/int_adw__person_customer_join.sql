with
    -- import CTES
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
    person_address as (
        select *
        from {{ ref('stg_adw__person_address') }}
    ),
    person_businessentity as (
        select *
        from {{ ref('stg_adw__person_businessentity') }}
    ),
    -- transformation
    joined as (
        select
            person_businessentity.entidade_empresa_pk,

            person_person.entidade_empresa_fk,
            person_person.nome_cliente,

            person_personphone.entidade_empresa_fk,
            person_personphone.numero_telefone,

            person_address.endereco_pk,
            --person_address.endereco_pessoa,
            --person_address.cidade_pessoa,
            --person_address.estado_fk,

            person_emailaddress.entidade_empresa_fk,
            person_emailaddress.email_pessoa
        from person_businessentity
        inner join person_person on person_businessentity.entidade_empresa_pk = person_person.entidade_empresa_fk
        inner join person_emailaddress on person_person.entidade_empresa_fk = person_emailaddress.entidade_empresa_fk
        inner join person_personphone on person_emailaddress.entidade_empresa_fk = person_personphone.entidade_empresa_fk
        inner join person_address on person_personphone.entidade_empresa_fk = person_address.endereco_pk
    )
select * from joined
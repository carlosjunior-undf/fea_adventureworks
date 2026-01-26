{{ config(
    materialized="view",
    schema="int_adw"
) }}

with

    person_person as (
        select *
        from {{ ref('stg_adw__person_person') }}
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
    sales_creditcard as (
        select *
        from {{ ref('stg_adw__sales_creditcard') }}
    ),
    sales_personcreditcard as (
        select *
        from {{ ref('stg_adw__sales_personcreditcard') }}
    ), 

    -- Após realizar dos joins, copie todas as colunas para a tabela de dimensão correspondente.
    
    joined as (
        select

        person_businessentityaddress.cliente_sk
        ,person_businessentityaddress.entidade_pessoa_fk
        ,person_businessentityaddress.endereco_fk
        --person_businessentityaddress.data_completa

        ,person_person.entidade_pessoa_pk
        ,person_person.nome_pessoa
        --person_person.data_completa

        ,person_emailaddress.email_pk
        --,person_emailaddress.entidade_pessoa_fk
        ,person_emailaddress.email_pessoa
        ,person_emailaddress.data_completa

        --,person_personphone.entidade_pessoa_fk
        ,person_personphone.telefone_pessoa
        --person_personphone.data_completa

        ,sales_creditcard.cartao_credito_pk
        ,sales_creditcard.tipo_cartao
        ,sales_creditcard.numero_cartao
        --sales_creditcard.data_completa

        --,sales_personcreditcard.entidade_pessoa_fk
        ,sales_personcreditcard.cartao_credito_fk
        --sales_personcreditcard.data_completa

        from person_person
        inner join person_emailaddress on person_person.entidade_pessoa_pk = person_emailaddress.entidade_pessoa_fk
        inner join person_businessentityaddress on person_emailaddress.entidade_pessoa_fk = person_businessentityaddress.entidade_pessoa_fk
        inner join person_personphone on person_businessentityaddress.entidade_pessoa_fk = person_personphone.entidade_pessoa_fk
        inner join sales_personcreditcard on person_personphone.entidade_pessoa_fk = sales_personcreditcard.entidade_pessoa_fk
        inner join sales_creditcard on sales_personcreditcard.cartao_credito_fk = sales_creditcard.cartao_credito_pk

    )
select * from joined
with
    -- import CTES
    sales_customer as (
        select *
        from {{ ref('stg_adw__sales_customer') }}
    ),
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
    -- transformation
    joined as (
        select
            --sales_customer.cliente_sk,
            sales_customer.cliente_pk,
            sales_customer.pessoa_pk,

            --person_person.entidade_empresa_pk,
            person_person.nome_cliente,

            person_personphone.entidade_empresa_fk,
            person_personphone.numero_telefone,

            person_address.entidade_empresa_fk,
            person_address.endereco_pk,

            person_emailaddress.email_pessoa,
            person_emailaddress.entidade_empresa_fk
        from sales_customer
        --inner join person_person on sales_customer.cliente_pk = person_person.entidade_empresa_pk
        inner join person_emailaddress on person_person.entidade_empresa_pk = person_emailaddress.entidade_empresa_fk
        inner join person_personphone on person_person.entidade_empresa_pk = person_personphone.entidade_empresa_fk
        inner join person_address on person_person.entidade_empresa_pk = person_address.endereco_pk
    )
select * from joined
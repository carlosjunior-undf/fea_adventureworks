{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_customer as (
        select * from {{ ref('int_adw__customer_join') }}
    ),
   
    customer__metrics as (
        select
            estado_id,
            endereco_id,
            entidade_pessoa_id
            territorio_id,
            email_id,

            endereco_pessoa,
            cep_pessoa,
            cidade_pessoa,
            codigo_estado,
            nome_estado,
            email_pessoa,
            nome_pessoa,
            telefone_pessoa,
            codigo_pais,
            nome_pais

        from int_customer

    )

select * from customer__metrics
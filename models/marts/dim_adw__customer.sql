{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    int_customer as (
        select * from {{ ref('int_adw__customer_join') }}
    ),
   
    customer_transformed as (
        select
            {{ dbt_utils.generate_surrogate_key(['entidade_pessoa_id', 'endereco_id','email_id',
            'estado_id','territorio_id']) }} as cliente_sk,
            entidade_pessoa_id,
            endereco_id,
            email_id,
            estado_id,
            territorio_id,
            
            nome_pessoa,
            telefone_pessoa,
            email_pessoa,
            endereco_pessoa,
            cep_pessoa,
            cidade_pessoa,
            codigo_estado,
            nome_estado,
            codigo_pais,
            nome_pais
        from int_customer
    )
select * from customer_transformed
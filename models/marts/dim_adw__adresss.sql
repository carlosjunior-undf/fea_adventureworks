{{ config(
    materialized="view",
    schema="dim_adw"
) }}

with
    int_address as (
        select *
        from {{ ref('int_adw__address_join') }}
    ),
    
    address_transformed as (
        select
        -- Traga todas as colunas da int_adw__address_join, mas deixe apenas a SK e as demais colunas ativas.

            endereco_sk
            ,endereco_pessoa
            ,cep_pessoa
            ,cidade_pessoa
            ,codigo_estado
            ,nome_estado
            ,codigo_pais
            ,nome_pais
            ,nome_territorio
            ,grupo_territorio
            ,data_completa

        from int_address
    )
select * from address_transformed
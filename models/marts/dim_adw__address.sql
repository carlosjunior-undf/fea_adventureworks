{{ config(
    materialized="view",
    schema="dim_adw"
) }}
with
    -- import CTES
    int_address as (
        select *
        from {{ ref('int_adw__address_join') }}
    ),
    
    dim_adw_address__metrics as (
        select
            localidade_sk,
            cidade_pessoa,
            --codigo_estado,
            nome_estado,
            --codigo_pais,
            nome_pais
        from int_address
    )
select * from dim_adw_address__metrics
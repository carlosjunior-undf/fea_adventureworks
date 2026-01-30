{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with
    int_location as (
        select *
        from {{ ref('int_adw__location_join') }}
    ),
    
    location_transformed as (
        select
        -- Traga todas as colunas da int_adw__address_join, mas deixe apenas a SK e as demais colunas ativas.

        localizacao_sk
        ,territorio_fk
        ,codigo_estado
        ,nome_estado
        ,nome_pais
        ,nome_territorio
        ,codigo_pais
        ,grupo_territorio

        from int_location
    )
select * from location_transformed
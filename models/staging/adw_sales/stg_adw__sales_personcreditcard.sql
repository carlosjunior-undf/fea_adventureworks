{{ config(
    materialized="view",
    schema="stg_adw"
) }}
with 

source_sales_personcreditcard as (

    select * from {{ source('adw_sales', 'sales_personcreditcard') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['businessentityid', 'creditcardid']) }} as cartao_credito_sk,
        cast(businessentityid as int) entidade_empresa_fk,
        cast(creditcardid as int) cartao_credito_fk,
        cast(modifieddate as date) as data_completa
    from source_sales_personcreditcard

)

select * from renamed
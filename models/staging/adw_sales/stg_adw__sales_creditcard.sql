    {{ config(
        materialized="view",
        schema="stg_adw"
    ) }}
with 

source_sales_creditcard as (

    select * from {{ source('adw_sales', 'sales_creditcard') }}

),

renamed as (
    select
        cast(creditcardid as int) as cartao_credito_id,
        cast(cardtype as string) as tipo_cartao,
        cast(cardnumber as string) as numero_cartao
        --cast(modifieddate as date) as modified_date
    from source_sales_creditcard

)

select * from renamed
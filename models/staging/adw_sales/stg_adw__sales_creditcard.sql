with 

source_sales_creditcard as (

    select * from {{ source('adw_sales', 'sales_creditcard') }}

),

renamed as (
    select
        cast(creditcardid as int) as cartao_credito_pk,
        cast(cardtype as string) as tipo_cartao,
        --cardnumber,
        cast(modifieddate as date) as modified_date
    from source_sales_creditcard

)

select * from renamed
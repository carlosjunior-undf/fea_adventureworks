with 

source_sales_creditcard as (

    select * from {{ source('adw_sales', 'sales_creditcard') }}

),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as cartao_credito_sk,
        cast(creditcardid as int) as cartao_credito_pk,
        cast(cardtype as string) as tipo_cartao
        --cardnumber,
        --cast(modifieddate as date) as modified_date
    from source_sales_creditcard

)

select * from renamed
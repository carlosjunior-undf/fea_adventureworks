with 

source as (

    select * from {{ source('adw_sales', 'sales_creditcard') }}

),

renamed as (

    select
        creditcardid,
        cardtype,
        cardnumber,
        expmonth,
        expyear,
        modifieddate

    from source

)

select * from renamed
with 

source as (

    select * from {{ source('adw_sales', 'sales_salesorderheadersalesreason') }}

),

renamed as (

    select
        salesorderid,
        salesreasonid,
        modifieddate

    from source

)

select * from renamed
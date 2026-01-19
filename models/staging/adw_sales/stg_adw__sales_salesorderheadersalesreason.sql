with 

source_sales_salesorderheadersalesreason as (

    select * from {{ source('adw_sales', 'sales_salesorderheadersalesreason') }}

),

renamed as (

    select
        cast(salesorderid as int) as salesorder_pk,
        cast(salesreasonid as int) as salesreason_fk,
        cast(modifieddate as date) as modified_date

    from source_sales_salesorderheadersalesreason

)

select * from renamed
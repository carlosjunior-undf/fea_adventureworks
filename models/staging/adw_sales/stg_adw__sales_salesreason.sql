with 

source_sales_salesreason as (

    select * from {{ source('adw_sales', 'sales_salesreason') }}

),

renamed as (

    select
        cast(salesreasonid as int) as salesreason_pk,
        cast(name as string) as reason_name,
        cast(reasontype as string) as reason_type,
        cast(modifieddate as date) as modified_date
    from source_sales_salesreason

)
select * from renamed
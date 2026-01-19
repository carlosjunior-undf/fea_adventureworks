with 

source_sales_salesorderdetail as (

    select * from {{ source('adw_sales', 'sales_salesorderdetail') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid', 'salesorderdetailid']) }} as sales_order_detail_sk,
        cast(salesorderid as int) as salesorder_fk,
        cast(salesorderdetailid as int) as salesorderdetail_fk,
        cast(productid as int) as product_fk,
        cast(modifieddate as date) as modified_date,
        cast(orderqty as int) as quantidade_comprada,
        unitprice,
        unitpricediscount,
        (unitprice * orderqty) as valor_total_negociado,
        (unitpricediscount * unitprice * orderqty) as descontos
    from source_sales_salesorderdetail

)

select * from renamed
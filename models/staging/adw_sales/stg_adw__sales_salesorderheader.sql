with 

source_sales_salesorderheader as (

    select * from {{ source('adw_sales', 'sales_salesorderheader') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid']) }} as sales_order_sk,
        cast(salesorderid as int) as salesorder_fk ,
        cast(customerid as int) as customer_fk,
        cast(salespersonid as int) as salesperson_fk,
        cast(territoryid as int) as territory_fk,
        cast(creditcardid as int) as creditcard_fk,
        cast(orderdate as date) as order_date ,
        cast(shipdate as date) as ship_date,
        status,
        subtotal,
        taxamt,
        freight,
        (subtotal + taxamt + freight) as faturamento_bruto,
        cast(modifieddate as date) as modified_date
    from source_sales_salesorderheader

)

select * from renamed
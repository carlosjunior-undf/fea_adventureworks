with 

source as (

    select * from {{ source('adw_sales', 'sales_salesorderheader') }}

),

renamed as (

    select
        salesorderid,
        revisionnumber,
        orderdate,
        duedate,
        shipdate,
        status,
        onlineorderflag,
        purchaseordernumber,
        accountnumber,
        customerid,
        salespersonid,
        territoryid,
        billtoaddressid,
        shiptoaddressid,
        shipmethodid,
        creditcardid,
        creditcardapprovalcode,
        currencyrateid,
        subtotal,
        taxamt,
        freight,
        totaldue,
        comment,
        rowguid,
        modifieddate

    from source

)

select * from renamed
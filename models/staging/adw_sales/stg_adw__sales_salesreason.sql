with 

source_sales_salesreason as (

    select * from {{ source('adw_sales', 'sales_salesreason') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesreasonid']) }} as motivo_venda_sk,
        cast(salesreasonid as int) as motivo_venda_pk,
        cast(name as string) as nome_motivo,
        cast(reasontype as string) as tipo_motivo
        --cast(modifieddate as date) as modified_date
    from source_sales_salesreason

)
select * from renamed
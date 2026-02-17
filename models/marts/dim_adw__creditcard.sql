{{ config(
    materialized="table",
    schema="dim_adw"
) }}
with
    int_creditcard as (
        select *
        from {{ ref('stg_adw__sales_creditcard') }}
    ),

    creditcard_transformed as (
        select

            {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as cartao_credito_sk
            ,creditcardid as cartao_credito_pk
            ,cardtype as tipo_cartao

        from int_creditcard
    )
select * from creditcard_transformed
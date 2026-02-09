{{ config(
    materialized="table",
    schema="fct_adw"
) }}
-- import dim dimentions
with
    int_salesorder as (
        select *
        from {{ ref('int_adw__salesorder_join') }}
    ),
    dim_customer as (
        select *
        from {{ ref('dim_adw__customer') }}
    ),
    dim_creditcard as (
        select *
        from {{ ref('dim_adw__creditcard') }}
    ),
    dim_product as (
        select *
        from {{ ref('dim_adw__product') }}
    ),
    dim_reason as (
        select *
        from {{ ref('dim_adw__reason') }}
    ),
    dim_status as (
        select *
        from {{ ref('dim_adw__status') }}
    ),
    dim_date as (
        select *
        from {{ ref('dim_adw__date') }}
    ),

    orders__metrics as (
        select
        {{ dbt_utils.generate_surrogate_key(['pedido_venda_pk']) }} as ordem_item_sk
        ,pedido_venda_pk
        ,detalhe_pedido_venda_pk

        ,dim_customer.cliente_sk as cliente_sk
        ,dim_creditcard.cartao_credito_sk as cartao_credito_sk
        ,dim_product.produto_sk as produto_sk
        ,dim_reason.motivo_venda_sk as motivo_venda_sk
        ,dim_status.status_sk as status_sk
        ,dim_date.data_sk as data_sk
            
        -- Demais colunas vindo da int_adw__salesorder_join.
            ,data_pedido
            ,data_vencimento
            ,data_envio

        -- Métricas.
            ,quantidade_comprada
            ,preco_unitario
            ,desconto_unitario
                ,case
                    when desconto_unitario > 0 then true
                    else false
                end as teve_desconto
            ,(preco_unitario * quantidade_comprada) as valor_total_negociado
            ,(preco_unitario * (1 - desconto_unitario) * quantidade_comprada) as desconto_total         

            ,sub_total
            ,taxa
            ,frete
            ,(sub_total + taxa + frete) as faturamento_bruto
            ,total_devido

        from int_salesorder
        inner join dim_customer on int_salesorder.territorio_fk = dim_customer.territorio_pk
        inner join dim_creditcard on int_salesorder.cartao_credito_fk = dim_creditcard.cartao_credito_pk
        inner join dim_product on int_salesorder.produto_fk = dim_product.produto_pk
        inner join dim_reason on int_salesorder.pedido_venda_pk = dim_reason.pedido_venda_fk
        inner join dim_status on int_salesorder.status_pk = dim_status.status_fk
        inner join dim_date on int_salesorder.data_completa_pk = dim_date.data_completa_pk
    )
    select * from orders__metrics
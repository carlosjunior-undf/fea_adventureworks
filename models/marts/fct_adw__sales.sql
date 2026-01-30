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
    dim_localition as (
        select *
        from {{ ref('dim_adw__location') }}
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
        -- Chaves SK da tabela fato e das dimensões, 
        -- convertidas como chaves FK para se conectarem à tabela fato.
            ordem_item_sk
            ,dim_customer.cliente_sk as cliente_fk
            ,dim_localition.localizacao_sk as territorio_fk
            ,dim_creditcard.cartao_credito_sk as cartao_credito_fk
            ,dim_product.produto_sk as produto_fk
            ,dim_reason.motivo_venda_sk as motivo_venda_fk
            ,dim_status.status_sk as status_fk
            ,dim_date.data_sk as data_completa_fk
            
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
        inner join dim_customer on int_salesorder.cliente_fk = dim_customer.cliente_fk
        inner join dim_localition on int_salesorder.territorio_fk = dim_localition.territorio_fk
        inner join dim_creditcard on int_salesorder.cartao_credito_fk = dim_creditcard.cartao_credito_fk
        inner join dim_product on int_salesorder.produto_fk = dim_product.produto_fk
        inner join dim_reason on int_salesorder.pedido_venda_fk = dim_reason.pedido_venda_fk
        inner join dim_status on int_salesorder.codigo_status = dim_status.codigo_status
        inner join dim_date on int_salesorder.data_completa = dim_date.data_completa
    )
    select * from orders__metrics
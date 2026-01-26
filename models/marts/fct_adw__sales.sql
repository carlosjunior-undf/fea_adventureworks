{{ config(
    materialized="view",
    schema="fct_adw"
) }}
-- import dim dimentions
with

    dim_date as (
        select *
        from {{ ref('dim_adw__date') }}
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
    int_salesorder as (
        select *
        from {{ ref('int_adw__salesorder_join') }}
    ),
    
    orders__metrics as (
        select
        -- Chave SK da tabela fato.
            ordem_item_sk
        -- Chaves FK das dimenções para conecta-las a tabela fato.
--            ,produto_fk
--            ,data_sk as data_fk 
--            ,motivo_venda_fk
--            ,status_fk
--            ,cliente_fk

        -- Demais colunas vindo da int_adw__salesorder_join.
            ,data_pedido
            ,data_vencimento
            ,data_envio
            ,codigo_status

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

            ,data_completa

        --Outras tabelas vindas das dimensões e que voce queira que conste ba tabela fato podem ser acrescentadas aqui.

        from int_salesorder
    )
    select * from orders__metrics
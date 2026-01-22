{{ config(
    materialized="view",
    schema="fct_adw"
) }}
with
    int_salesorder as (
        select *
        from {{ ref('int_adw__salesorder_join') }}
    ),
    
    fct_adw_int_salesorder__metrics as (
        select
            pedido_venda_sk,
            pedido_venda_pk,
            pessoa_venda_pk,
            cliente_fk,
--          territorio_fk,
            cartao_credito_fk,
            data_pedido,
            data_envio,
--          data_completa,
            codigo_status,
            sub_total,
            taxa,
            frete,
--            faturamento_bruto,
            (sub_total + taxa + frete) as faturamento_bruto,

            pedido_venda_item_sk,
            pedido_venda_fk,
            pedido_venda_item_fk,
            produto_fk,
            data_completa,
            quantidade_comprada,
            preco_unitario,
            desconto_pct,
--            valor_total_negociado,
--            desconto_total,
--            teve_desconto,
            case
                when desconto_pct > 0 then true
                else false
            end as teve_desconto,
            (preco_unitario * quantidade_comprada) as valor_total_negociado,
            (preco_unitario * (1 - desconto_pct) * quantidade_comprada) as desconto_total,

            cliente_sk,
            cliente_pk,
            pessoa_pk,
            territorio_fk
--          sales_customer.data_completa

        
        from int_salesorder
    )
select * from fct_adw_int_salesorder__metrics
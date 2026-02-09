with 
    fato_vendas as (
        select *
        from {{ ref('fct_adw__sales') }}
    ),

faturamento_2011 as (
    select
        round(sum(faturamento_bruto), 2) as total_faturamento
    from fato_vendas 
    where data_pedido between "2011-01-01" and "2011-12-31" 
)
select *
from faturamento_2011
where total_faturamento != 42929544721.0

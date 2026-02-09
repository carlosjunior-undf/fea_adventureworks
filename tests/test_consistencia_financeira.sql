with 
    fato_vendas as (
        select *
        from {{ ref('fct_adw__sales') }}
    ),

faturamento_2011 as (
    select *
    from fato_vendas 
    where quantidade_comprada < 0
    OR faturamento_bruto < 0
    OR desconto_total < 0
 
)
select *
from faturamento_2011
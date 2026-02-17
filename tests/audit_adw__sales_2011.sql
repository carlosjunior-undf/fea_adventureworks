
with fct_sales as (

    select *
    from {{ ref('fct_adw__sales') }}

),

date_dim as (

    select *
    from {{ ref('dim_adw__date') }}

),

teste_pedido_ceo as (

    select
        round(sum(f.faturamento_bruto),2) as faturamento_2011
    from fct_sales f
    join date_dim d
        on f.data_sk = d.data_sk
    where d.ano = 2011

)

select *
from teste_pedido_ceo
where faturamento_2011 != 12646112.16

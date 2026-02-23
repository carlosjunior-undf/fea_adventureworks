-- tests/assert_gross_revenue_2011.sql
-- Teste de auditoria solicitado pelo CEO Carlos Silveira.
-- Valida que a receita bruta total do ano de 2011 é igual a $12.646.112,16.
-- O teste passa quando a query retorna ZERO linhas (sem discrepância).

with actual as (

    select
        sum(gross_revenue) as total_gross_revenue_2011

    from {{ ref('fct_adw__sales') }}

    where year(orderdate) = 2011

),

expected as (

    select 12646112.16 as expected_value

),

validation as (

    select
        a.total_gross_revenue_2011
        ,e.expected_value
        ,round(a.total_gross_revenue_2011 - e.expected_value, 2) as variance

    from actual a
    cross join expected e

)

-- Retorna linha se há discrepância — dbt interpreta retorno de linha como falha
select *
from validation
where abs(variance) > 1   -- tolerância de 1 dolar para arredondamento

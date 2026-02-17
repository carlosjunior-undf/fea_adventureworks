{{config(
    materialized="table",
    schema="dim_adw"
)}}

select 1 as status_sk,1 as status_pk,'Em Processamento' as status union all
select 2,2,'Aprovado' union all
select 3,3,'Retornado' union all
select 4,4,'Rejeitado' union all
select 5,5,'Enviado' union all
select 6,6,'Cancelado'
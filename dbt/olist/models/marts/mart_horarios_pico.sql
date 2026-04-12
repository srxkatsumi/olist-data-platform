-- mart_horarios_pico.sql
-- Pergunta: Quais horários têm mais compras?
{{ config(materialized='table') }}

with pedidos as (
    select * from {{ ref('stg_orders') }}
),
resultado as (
    select
        ORDER_HOUR_OF_DAY                as HORA_DO_DIA,
        ORDER_DAY_OF_WEEK                as DIA_DA_SEMANA,
        count(ORDER_ID)                  as TOTAL_PEDIDOS,
        round(avg(case 
            when ORDER_STATUS = 'delivered' 
            then datediff('day', ORDER_PURCHASED_AT, ORDER_DELIVERED_AT)
        end), 1)                         as MEDIA_DIAS_ENTREGA
    from pedidos
    where ORDER_PURCHASED_AT is not null
    group by HORA_DO_DIA, DIA_DA_SEMANA
    order by TOTAL_PEDIDOS desc
)
select * from resultado

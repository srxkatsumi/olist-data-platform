-- mart_sazonalidade.sql
-- Executed by: dbt
-- Stored in: Snowflake (RAW schema as TABLE)
-- Question: What is the sales seasonality period?

{{ config(materialized='table') }}

with pedidos as (
    select * from {{ ref('stg_orders') }}
),

sazonalidade as (
    select
        -- Period
        ORDER_MONTH                          as MES,
        year(ORDER_MONTH)                    as ANO,
        month(ORDER_MONTH)                   as NUMERO_MES,

        -- Metrics
        count(ORDER_ID)                      as TOTAL_PEDIDOS,
        count(case when ORDER_STATUS = 'delivered' 
              then 1 end)                    as PEDIDOS_ENTREGUES,
        count(case when ORDER_STATUS = 'canceled' 
              then 1 end)                    as PEDIDOS_CANCELADOS,

        -- Cancellation rate
        round(
            count(case when ORDER_STATUS = 'canceled' then 1 end) * 100.0 
            / count(ORDER_ID), 2
        )                                    as TAXA_CANCELAMENTO_PCT

    from pedidos
    where ORDER_MONTH is not null
    group by ORDER_MONTH
    order by ORDER_MONTH
)

select * from sazonalidade

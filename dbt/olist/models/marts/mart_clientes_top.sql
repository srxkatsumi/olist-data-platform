-- mart_clientes_top.sql
-- Question: Which customers buy the most?
{{ config(materialized='table') }}

with pedidos as (
    select * from {{ ref('stg_orders') }}
),
pagamentos as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),
clientes as (
    select * from {{ source('raw', 'OLIST_CUSTOMERS_DATASET') }}
),
resultado as (
    select
        p.CUSTOMER_ID,
        c.CUSTOMER_CITY                        as CIDADE,
        c.CUSTOMER_STATE                       as ESTADO,
        count(p.ORDER_ID)                      as TOTAL_PEDIDOS,
        round(sum(pg.PAYMENT_VALUE::float), 2) as VALOR_TOTAL_GASTO,
        round(avg(pg.PAYMENT_VALUE::float), 2) as TICKET_MEDIO,
        min(p.ORDER_PURCHASED_AT)              as PRIMEIRA_COMPRA,
        max(p.ORDER_PURCHASED_AT)              as ULTIMA_COMPRA
    from pedidos p
    left join clientes c on p.CUSTOMER_ID = c.CUSTOMER_ID
    left join pagamentos pg on p.ORDER_ID = pg.ORDER_ID
    where p.ORDER_STATUS = 'delivered'
    group by p.CUSTOMER_ID, c.CUSTOMER_CITY, c.CUSTOMER_STATE
    order by VALOR_TOTAL_GASTO desc
)
select * from resultado
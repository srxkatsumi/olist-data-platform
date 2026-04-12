-- mart_perfil_cliente.sql
-- Question: What is the target audience profile?
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
        c.CUSTOMER_STATE                        as ESTADO,
        c.CUSTOMER_CITY                         as CIDADE,
        count(distinct p.CUSTOMER_ID)           as TOTAL_CLIENTES,
        count(p.ORDER_ID)                       as TOTAL_PEDIDOS,
        round(avg(pg.PAYMENT_VALUE::float), 2)  as TICKET_MEDIO,
        round(sum(pg.PAYMENT_VALUE::float), 2)  as RECEITA_TOTAL,
        -- Most used payment method
        mode(pg.PAYMENT_TYPE)                   as PAGAMENTO_PREFERIDO
    from pedidos p
    left join clientes c on p.CUSTOMER_ID = c.CUSTOMER_ID
    left join pagamentos pg on p.ORDER_ID = pg.ORDER_ID
    where p.ORDER_STATUS = 'delivered'
    group by c.CUSTOMER_STATE, c.CUSTOMER_CITY
    order by RECEITA_TOTAL desc
)
select * from resultado

-- 09_customers_above_avg_ticket.sql
-- Question: What is the avg ticket and how many customers spend above it?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

customer_spending as (
    select
        o.CUSTOMER_ID,
        count(distinct o.ORDER_ID)              as TOTAL_ORDERS,
        round(sum(p.PAYMENT_VALUE::float), 2)   as TOTAL_SPENT,
        round(avg(p.PAYMENT_VALUE::float), 2)   as AVG_ORDER_VALUE
    from orders o
    inner join payments p on o.ORDER_ID = p.ORDER_ID
    group by 1
),

global_avg as (
    select round(avg(AVG_ORDER_VALUE), 2) as GLOBAL_AVG_TICKET
    from customer_spending
),

result as (
    select
        g.GLOBAL_AVG_TICKET,
        count(distinct cs.CUSTOMER_ID)                              as TOTAL_CUSTOMERS,
        count(distinct case when cs.AVG_ORDER_VALUE > g.GLOBAL_AVG_TICKET
              then cs.CUSTOMER_ID end)                              as CUSTOMERS_ABOVE_AVG,
        count(distinct case when cs.AVG_ORDER_VALUE <= g.GLOBAL_AVG_TICKET
              then cs.CUSTOMER_ID end)                              as CUSTOMERS_BELOW_AVG,
        round(count(distinct case when cs.AVG_ORDER_VALUE > g.GLOBAL_AVG_TICKET
              then cs.CUSTOMER_ID end) * 100.0
              / nullif(count(distinct cs.CUSTOMER_ID), 0), 2)       as PCT_ABOVE_AVG
    from customer_spending cs
    cross join global_avg g
    group by 1
)

select * from result

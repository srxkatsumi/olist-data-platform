-- 11_lifetime_value_by_state.sql
-- Question: Which states have the highest lifetime value per customer?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

customers as (
    select * from {{ source('raw', 'OLIST_CUSTOMERS_DATASET') }}
),

customer_ltv as (
    select
        o.CUSTOMER_ID,
        sum(p.PAYMENT_VALUE::float)     as LIFETIME_VALUE,
        count(distinct o.ORDER_ID)      as TOTAL_ORDERS
    from orders o
    inner join payments p on o.ORDER_ID = p.ORDER_ID
    group by 1
),

result as (
    select
        c.CUSTOMER_STATE                                as STATE,
        count(distinct cl.CUSTOMER_ID)                  as TOTAL_CUSTOMERS,
        round(avg(cl.LIFETIME_VALUE), 2)                as AVG_LTV,
        round(max(cl.LIFETIME_VALUE), 2)                as MAX_LTV,
        round(min(cl.LIFETIME_VALUE), 2)                as MIN_LTV,
        round(avg(cl.TOTAL_ORDERS), 1)                  as AVG_ORDERS
    from customer_ltv cl
    inner join customers c on cl.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1
    order by AVG_LTV desc
)

select * from result

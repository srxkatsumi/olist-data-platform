-- 10_top_customers_geographic_profile.sql
-- Question: What is the geographic profile of top customers?

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

customer_value as (
    select
        o.CUSTOMER_ID,
        round(sum(p.PAYMENT_VALUE::float), 2)   as LIFETIME_VALUE,
        count(distinct o.ORDER_ID)              as TOTAL_ORDERS
    from orders o
    inner join payments p on o.ORDER_ID = p.ORDER_ID
    group by 1
),

result as (
    select
        c.CUSTOMER_STATE                            as STATE,
        count(distinct cv.CUSTOMER_ID)              as TOTAL_CUSTOMERS,
        round(avg(cv.LIFETIME_VALUE), 2)            as AVG_LIFETIME_VALUE,
        round(sum(cv.LIFETIME_VALUE), 2)            as TOTAL_REVENUE,
        round(avg(cv.TOTAL_ORDERS), 1)              as AVG_ORDERS_PER_CUSTOMER,
        round(sum(cv.LIFETIME_VALUE) * 100.0
            / sum(sum(cv.LIFETIME_VALUE)) over(), 2) as REVENUE_SHARE_PCT
    from customer_value cv
    inner join customers c on cv.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1
    order by TOTAL_REVENUE desc
)

select * from result

-- 08_single_purchase_customers.sql
-- Question: Which customers bought only once? (churn risk)

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

customers as (
    select * from {{ source('raw', 'OLIST_CUSTOMERS_DATASET') }}
),

order_count as (
    select
        o.CUSTOMER_ID,
        count(distinct o.ORDER_ID)  as TOTAL_ORDERS,
        min(o.ORDER_PURCHASED_AT)   as FIRST_ORDER,
        max(o.ORDER_PURCHASED_AT)   as LAST_ORDER
    from orders o
    group by 1
),

result as (
    select
        c.CUSTOMER_STATE                                        as STATE,
        c.CUSTOMER_CITY                                         as CITY,
        count(distinct case when oc.TOTAL_ORDERS = 1
              then oc.CUSTOMER_ID end)                          as SINGLE_PURCHASE_CUSTOMERS,
        count(distinct oc.CUSTOMER_ID)                          as TOTAL_CUSTOMERS,
        round(count(distinct case when oc.TOTAL_ORDERS = 1
              then oc.CUSTOMER_ID end) * 100.0
              / nullif(count(distinct oc.CUSTOMER_ID), 0), 2)   as CHURN_RATE_PCT
    from order_count oc
    inner join customers c on oc.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1, 2
    having count(distinct oc.CUSTOMER_ID) >= 50
    order by CHURN_RATE_PCT desc
    limit 20
)

select * from result

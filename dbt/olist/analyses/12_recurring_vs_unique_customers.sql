-- 12_recurring_vs_unique_customers.sql
-- Question: How many customers are recurring vs one-time buyers per state?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

customers as (
    select * from {{ source('raw', 'OLIST_CUSTOMERS_DATASET') }}
),

order_count as (
    select
        CUSTOMER_ID,
        count(distinct ORDER_ID) as TOTAL_ORDERS
    from orders
    group by 1
),

result as (
    select
        c.CUSTOMER_STATE                                                as STATE,
        count(distinct oc.CUSTOMER_ID)                                  as TOTAL_CUSTOMERS,
        count(distinct case when oc.TOTAL_ORDERS = 1
              then oc.CUSTOMER_ID end)                                  as ONE_TIME_BUYERS,
        count(distinct case when oc.TOTAL_ORDERS > 1
              then oc.CUSTOMER_ID end)                                  as RECURRING_BUYERS,
        round(count(distinct case when oc.TOTAL_ORDERS > 1
              then oc.CUSTOMER_ID end) * 100.0
              / nullif(count(distinct oc.CUSTOMER_ID), 0), 2)           as RECURRING_RATE_PCT
    from order_count oc
    inner join customers c on oc.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1
    order by RECURRING_RATE_PCT desc
)

select * from result

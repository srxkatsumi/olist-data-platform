-- 06_payment_method_by_state.sql
-- Question: What are the preferred payment methods by state?

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

result as (
    select
        c.CUSTOMER_STATE                                as STATE,
        p.PAYMENT_TYPE                                  as PAYMENT_METHOD,
        count(distinct o.ORDER_ID)                      as TOTAL_ORDERS,
        round(sum(p.PAYMENT_VALUE::float), 2)           as TOTAL_REVENUE,
        round(avg(p.PAYMENT_VALUE::float), 2)           as AVG_ORDER_VALUE,
        round(count(distinct o.ORDER_ID) * 100.0
            / sum(count(distinct o.ORDER_ID))
              over (partition by c.CUSTOMER_STATE), 2)  as PAYMENT_SHARE_PCT
    from orders o
    inner join payments p   on o.ORDER_ID = p.ORDER_ID
    inner join customers c  on o.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1, 2
    order by STATE, TOTAL_ORDERS desc
)

select * from result

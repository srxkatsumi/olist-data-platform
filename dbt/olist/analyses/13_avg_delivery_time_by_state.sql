-- 13_avg_delivery_time_by_state.sql
-- Question: What is the average delivery time in days by state?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_PURCHASED_AT is not null
),

customers as (
    select * from {{ source('raw', 'OLIST_CUSTOMERS_DATASET') }}
),

result as (
    select
        c.CUSTOMER_STATE                                            as STATE,
        count(distinct o.ORDER_ID)                                  as TOTAL_ORDERS,
        round(avg(datediff('day',
            o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT)), 1)        as AVG_DELIVERY_DAYS,
        round(min(datediff('day',
            o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT)), 1)        as MIN_DELIVERY_DAYS,
        round(max(datediff('day',
            o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT)), 1)        as MAX_DELIVERY_DAYS,
        round(avg(datediff('day',
            o.ORDER_PURCHASED_AT, o.ORDER_ESTIMATED_AT)), 1)        as AVG_ESTIMATED_DAYS
    from orders o
    inner join customers c on o.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1
    order by AVG_DELIVERY_DAYS desc
)

select * from result

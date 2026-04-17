-- 15_delay_rate_by_state.sql
-- Question: Which states have the highest delivery delay rate?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_ESTIMATED_AT is not null
),

customers as (
    select * from {{ source('raw', 'OLIST_CUSTOMERS_DATASET') }}
),

result as (
    select
        c.CUSTOMER_STATE                                            as STATE,
        count(distinct o.ORDER_ID)                                  as TOTAL_ORDERS,
        count(distinct case when o.ORDER_DELIVERED_AT > o.ORDER_ESTIMATED_AT
              then o.ORDER_ID end)                                  as LATE_ORDERS,
        round(count(distinct case when o.ORDER_DELIVERED_AT > o.ORDER_ESTIMATED_AT
              then o.ORDER_ID end) * 100.0
              / nullif(count(distinct o.ORDER_ID), 0), 2)           as LATE_RATE_PCT,
        round(avg(case when o.ORDER_DELIVERED_AT > o.ORDER_ESTIMATED_AT
              then datediff('day', o.ORDER_ESTIMATED_AT, o.ORDER_DELIVERED_AT)
              end), 1)                                              as AVG_DAYS_LATE
    from orders o
    inner join customers c on o.CUSTOMER_ID = c.CUSTOMER_ID
    group by 1
    order by LATE_RATE_PCT desc
)

select * from result

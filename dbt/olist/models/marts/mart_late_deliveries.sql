-- mart_late_deliveries.sql
-- Question: How many orders were delivered late vs the estimated date?
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_ESTIMATED_AT is not null
),

result as (
    select
        year(ORDER_PURCHASED_AT)                                    as YEAR,
        month(ORDER_PURCHASED_AT)                                   as MONTH,
        monthname(ORDER_PURCHASED_AT)                               as MONTH_NAME,
        count(distinct ORDER_ID)                                    as TOTAL_ORDERS,
        count(distinct case when ORDER_DELIVERED_AT > ORDER_ESTIMATED_AT
              then ORDER_ID end)                                    as LATE_ORDERS,
        count(distinct case when ORDER_DELIVERED_AT <= ORDER_ESTIMATED_AT
              then ORDER_ID end)                                    as ON_TIME_ORDERS,
        round(count(distinct case when ORDER_DELIVERED_AT > ORDER_ESTIMATED_AT
              then ORDER_ID end) * 100.0
              / nullif(count(distinct ORDER_ID), 0), 2)             as LATE_RATE_PCT,
        round(avg(datediff('day',
            ORDER_ESTIMATED_AT, ORDER_DELIVERED_AT)), 1)            as AVG_DAYS_LATE
    from orders
    group by 1, 2, 3
    order by 1, 2
)

select * from result

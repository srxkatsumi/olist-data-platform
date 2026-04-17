-- 19_sellers_with_highest_delay.sql
-- Question: Which sellers have the highest delivery delay rate?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_ESTIMATED_AT is not null
),

items as (
    select * from {{ source('raw', 'OLIST_ORDER_ITEMS_DATASET') }}
),

sellers as (
    select * from {{ source('raw', 'OLIST_SELLERS_DATASET') }}
),

result as (
    select
        i.SELLER_ID,
        s.SELLER_CITY                                               as CITY,
        s.SELLER_STATE                                              as STATE,
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
    inner join items i   on o.ORDER_ID = i.ORDER_ID
    inner join sellers s on i.SELLER_ID = s.SELLER_ID
    group by 1, 2, 3
    having count(distinct o.ORDER_ID) >= 30
    order by LATE_RATE_PCT desc
    limit 20
)

select * from result

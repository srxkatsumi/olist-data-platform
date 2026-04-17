-- 21_active_sellers_by_state.sql
-- Question: Which states have the most active sellers?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

items as (
    select * from {{ source('raw', 'OLIST_ORDER_ITEMS_DATASET') }}
),

sellers as (
    select * from {{ source('raw', 'OLIST_SELLERS_DATASET') }}
),

result as (
    select
        s.SELLER_STATE                                      as STATE,
        count(distinct i.SELLER_ID)                         as ACTIVE_SELLERS,
        count(distinct o.ORDER_ID)                          as TOTAL_ORDERS,
        round(sum(i.PRICE::float), 2)                       as TOTAL_REVENUE,
        round(avg(i.PRICE::float), 2)                       as AVG_ITEM_PRICE,
        round(count(distinct o.ORDER_ID) * 1.0
            / nullif(count(distinct i.SELLER_ID), 0), 1)    as ORDERS_PER_SELLER
    from orders o
    inner join items i    on o.ORDER_ID = i.ORDER_ID
    inner join sellers s  on i.SELLER_ID = s.SELLER_ID
    group by 1
    order by ACTIVE_SELLERS desc
)

select * from result

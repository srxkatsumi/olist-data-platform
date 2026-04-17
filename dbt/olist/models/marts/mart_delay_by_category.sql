-- mart_delay_by_category.sql
-- Question: Which product categories have the most delivery delays?
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_ESTIMATED_AT is not null
),

items as (
    select * from {{ source('raw', 'OLIST_ORDER_ITEMS_DATASET') }}
),

products as (
    select * from {{ source('raw', 'OLIST_PRODUCTS_DATASET') }}
),

translation as (
    select * from {{ source('raw', 'PRODUCT_CATEGORY_NAME_TRANSLATION') }}
),

result as (
    select
        coalesce(t.PRODUCT_CATEGORY_NAME_ENGLISH,
            p.PRODUCT_CATEGORY_NAME, 'unknown')                     as CATEGORY,
        count(distinct o.ORDER_ID)                                  as TOTAL_ORDERS,
        count(distinct case when o.ORDER_DELIVERED_AT > o.ORDER_ESTIMATED_AT
              then o.ORDER_ID end)                                  as LATE_ORDERS,
        round(count(distinct case when o.ORDER_DELIVERED_AT > o.ORDER_ESTIMATED_AT
              then o.ORDER_ID end) * 100.0
              / nullif(count(distinct o.ORDER_ID), 0), 2)           as LATE_RATE_PCT,
        round(avg(datediff('day',
            o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT)), 1)        as AVG_DELIVERY_DAYS
    from orders o
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
    having count(distinct o.ORDER_ID) >= 100
    order by LATE_RATE_PCT desc
)

select * from result

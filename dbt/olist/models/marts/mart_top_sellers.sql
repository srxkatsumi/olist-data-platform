-- mart_top_sellers.sql
-- Question: Who are the top sellers by revenue and order volume?
{{ config(materialized='table') }}

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
        i.SELLER_ID,
        s.SELLER_CITY                               as CITY,
        s.SELLER_STATE                              as STATE,
        count(distinct o.ORDER_ID)                  as TOTAL_ORDERS,
        count(distinct i.PRODUCT_ID)                as UNIQUE_PRODUCTS,
        round(sum(i.PRICE::float), 2)               as TOTAL_REVENUE,
        round(avg(i.PRICE::float), 2)               as AVG_ITEM_PRICE,
        round(sum(i.PRICE::float) * 100.0
            / sum(sum(i.PRICE::float)) over(), 2)   as REVENUE_SHARE_PCT
    from orders o
    inner join items i    on o.ORDER_ID = i.ORDER_ID
    inner join sellers s  on i.SELLER_ID = s.SELLER_ID
    group by 1, 2, 3
    order by TOTAL_REVENUE desc
    limit 20
)

select * from result

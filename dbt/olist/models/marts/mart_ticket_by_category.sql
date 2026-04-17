-- mart_ticket_by_category.sql
-- Question: What is the average ticket per product category?
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
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
            p.PRODUCT_CATEGORY_NAME, 'unknown')             as CATEGORY,
        count(distinct o.ORDER_ID)                          as TOTAL_ORDERS,
        round(sum(i.PRICE::float), 2)                       as TOTAL_REVENUE,
        round(avg(i.PRICE::float), 2)                       as AVG_ITEM_PRICE,
        round(min(i.PRICE::float), 2)                       as MIN_PRICE,
        round(max(i.PRICE::float), 2)                       as MAX_PRICE
    from orders o
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
    having count(distinct o.ORDER_ID) >= 100
    order by AVG_ITEM_PRICE desc
)

select * from result

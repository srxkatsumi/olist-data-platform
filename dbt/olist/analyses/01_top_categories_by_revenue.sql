-- 01_top_categories_by_revenue.sql
-- Question: Which are the top 10 categories by revenue and order volume?

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
        coalesce(t.PRODUCT_CATEGORY_NAME_ENGLISH, p.PRODUCT_CATEGORY_NAME, 'unknown') as CATEGORY,
        count(distinct o.ORDER_ID)                 as TOTAL_ORDERS,
        count(distinct i.PRODUCT_ID)               as UNIQUE_PRODUCTS,
        round(sum(i.PRICE::float), 2)              as TOTAL_REVENUE,
        round(avg(i.PRICE::float), 2)              as AVG_PRICE,
        round(sum(i.PRICE::float) * 100.0
            / sum(sum(i.PRICE::float)) over (), 2) as REVENUE_SHARE_PCT
    from orders o
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
    order by TOTAL_REVENUE desc
    limit 10
)

select * from result

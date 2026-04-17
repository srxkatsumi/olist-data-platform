-- mart_reviews_by_category.sql
-- Question: Which product categories have the worst review scores?
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

reviews as (
    select * from {{ source('raw', 'OLIST_ORDER_REVIEWS_DATASET') }}
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
            p.PRODUCT_CATEGORY_NAME, 'unknown')                 as CATEGORY,
        count(distinct o.ORDER_ID)                              as TOTAL_ORDERS,
        round(avg(r.REVIEW_SCORE::float), 2)                    as AVG_SCORE,
        count(distinct case when r.REVIEW_SCORE = 5
              then o.ORDER_ID end)                              as FIVE_STAR,
        count(distinct case when r.REVIEW_SCORE = 1
              then o.ORDER_ID end)                              as ONE_STAR,
        round(count(distinct case when r.REVIEW_SCORE <= 2
              then o.ORDER_ID end) * 100.0
              / nullif(count(distinct o.ORDER_ID), 0), 2)       as NEGATIVE_RATE_PCT,
        round(count(distinct case when r.REVIEW_SCORE >= 4
              then o.ORDER_ID end) * 100.0
              / nullif(count(distinct o.ORDER_ID), 0), 2)       as POSITIVE_RATE_PCT
    from orders o
    inner join reviews r    on o.ORDER_ID = r.ORDER_ID
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
    having count(distinct o.ORDER_ID) >= 100
    order by AVG_SCORE asc
)

select * from result

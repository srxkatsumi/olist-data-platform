-- 03_cancellation_rate_by_category.sql
-- Question: Which categories have the highest cancellation rate?

with orders as (
    select * from {{ ref('stg_orders') }}
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
        count(distinct o.ORDER_ID)                                    as TOTAL_ORDERS,
        count(distinct case when o.ORDER_STATUS = 'canceled'
              then o.ORDER_ID end)                                    as CANCELED_ORDERS,
        round(
            count(distinct case when o.ORDER_STATUS = 'canceled'
                  then o.ORDER_ID end) * 100.0
            / nullif(count(distinct o.ORDER_ID), 0), 2
        )                                                             as CANCELLATION_RATE_PCT
    from orders o
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
    having count(distinct o.ORDER_ID) >= 100
    order by CANCELLATION_RATE_PCT desc
    limit 15
)

select * from result

-- mart_top_category_by_month.sql
-- Question: Which category sells the most each month?
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

monthly_category as (
    select
        year(o.ORDER_PURCHASED_AT)                                  as YEAR,
        month(o.ORDER_PURCHASED_AT)                                 as MONTH,
        monthname(o.ORDER_PURCHASED_AT)                             as MONTH_NAME,
        coalesce(t.PRODUCT_CATEGORY_NAME_ENGLISH,
            p.PRODUCT_CATEGORY_NAME, 'unknown')                     as CATEGORY,
        count(distinct o.ORDER_ID)                                  as TOTAL_ORDERS,
        round(sum(i.PRICE::float), 2)                               as TOTAL_REVENUE
    from orders o
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1, 2, 3, 4
),

ranked as (
    select
        YEAR,
        MONTH,
        MONTH_NAME,
        CATEGORY,
        TOTAL_ORDERS,
        TOTAL_REVENUE,
        row_number() over (
            partition by YEAR, MONTH
            order by TOTAL_REVENUE desc
        ) as RANK
    from monthly_category
)

select
    YEAR,
    MONTH,
    MONTH_NAME,
    CATEGORY        as TOP_CATEGORY,
    TOTAL_ORDERS,
    TOTAL_REVENUE
from ranked
where RANK = 1
order by YEAR, MONTH

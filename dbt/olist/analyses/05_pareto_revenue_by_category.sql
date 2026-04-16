-- 05_pareto_revenue_by_category.sql
-- Question: Which categories concentrate 80% of total revenue? (Pareto analysis)

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

category_revenue as (
    select
        coalesce(t.PRODUCT_CATEGORY_NAME_ENGLISH, p.PRODUCT_CATEGORY_NAME, 'unknown') as CATEGORY,
        round(sum(i.PRICE::float), 2) as TOTAL_REVENUE
    from orders o
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products p   on i.PRODUCT_ID = p.PRODUCT_ID
    left join translation t on p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
),

result as (
    select
        CATEGORY,
        TOTAL_REVENUE,
        round(TOTAL_REVENUE * 100.0 / sum(TOTAL_REVENUE) over (), 2)    as REVENUE_SHARE_PCT,
        round(sum(TOTAL_REVENUE) over (order by TOTAL_REVENUE desc
              rows between unbounded preceding and current row)
              * 100.0 / sum(TOTAL_REVENUE) over (), 2)                  as CUMULATIVE_SHARE_PCT
    from category_revenue
    order by TOTAL_REVENUE desc
)

select * from result

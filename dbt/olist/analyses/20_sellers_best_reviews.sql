-- 20_sellers_best_reviews.sql
-- Question: Which sellers have the best and worst review scores?

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

reviews as (
    select * from {{ source('raw', 'OLIST_ORDER_REVIEWS_DATASET') }}
),

result as (
    select
        i.SELLER_ID,
        s.SELLER_CITY                               as CITY,
        s.SELLER_STATE                              as STATE,
        count(distinct o.ORDER_ID)                  as TOTAL_ORDERS,
        round(avg(r.REVIEW_SCORE::float), 2)        as AVG_REVIEW_SCORE,
        count(distinct case when r.REVIEW_SCORE = 5
              then o.ORDER_ID end)                  as FIVE_STAR_ORDERS,
        count(distinct case when r.REVIEW_SCORE = 1
              then o.ORDER_ID end)                  as ONE_STAR_ORDERS,
        round(count(distinct case when r.REVIEW_SCORE >= 4
              then o.ORDER_ID end) * 100.0
              / nullif(count(distinct o.ORDER_ID), 0), 2) as POSITIVE_RATE_PCT
    from orders o
    inner join items i    on o.ORDER_ID = i.ORDER_ID
    inner join sellers s  on i.SELLER_ID = s.SELLER_ID
    inner join reviews r  on o.ORDER_ID = r.ORDER_ID
    group by 1, 2, 3
    having count(distinct o.ORDER_ID) >= 30
    order by AVG_REVIEW_SCORE desc
    limit 20
)

select * from result

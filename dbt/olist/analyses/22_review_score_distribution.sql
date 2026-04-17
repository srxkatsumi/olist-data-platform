-- 22_review_score_distribution.sql
-- Question: What is the distribution of review scores (1 to 5 stars)?

with reviews as (
    select * from {{ source('raw', 'OLIST_ORDER_REVIEWS_DATASET') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

result as (
    select
        r.REVIEW_SCORE                                          as SCORE,
        count(distinct r.REVIEW_ID)                             as TOTAL_REVIEWS,
        round(count(distinct r.REVIEW_ID) * 100.0
            / sum(count(distinct r.REVIEW_ID)) over(), 2)       as SHARE_PCT,
        round(avg(datediff('day',
            o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT)), 1)    as AVG_DELIVERY_DAYS
    from reviews r
    inner join orders o on r.ORDER_ID = o.ORDER_ID
    group by 1
    order by 1 desc
)

select * from result

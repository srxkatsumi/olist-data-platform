-- 24_delivery_time_vs_review.sql
-- Question: What is the correlation between delivery time and review score?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_PURCHASED_AT is not null
),

reviews as (
    select * from {{ source('raw', 'OLIST_ORDER_REVIEWS_DATASET') }}
),

result as (
    select
        case
            when datediff('day',
                o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT) <= 3  then '1-3 days'
            when datediff('day',
                o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT) <= 7  then '4-7 days'
            when datediff('day',
                o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT) <= 14 then '8-14 days'
            when datediff('day',
                o.ORDER_PURCHASED_AT, o.ORDER_DELIVERED_AT) <= 21 then '15-21 days'
            else '22+ days'
        end                                                         as DELIVERY_RANGE,
        count(distinct o.ORDER_ID)                                  as TOTAL_ORDERS,
        round(avg(r.REVIEW_SCORE::float), 2)                        as AVG_SCORE,
        round(count(distinct case when r.REVIEW_SCORE >= 4
              then o.ORDER_ID end) * 100.0
              / nullif(count(distinct o.ORDER_ID), 0), 2)           as POSITIVE_RATE_PCT
    from orders o
    inner join reviews r on o.ORDER_ID = r.ORDER_ID
    group by 1
    order by AVG_SCORE desc
)

select * from result

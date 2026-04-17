-- 25_seller_response_time_vs_review.sql
-- Question: Does seller response time affect the review score?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

reviews as (
    select * from {{ source('raw', 'OLIST_ORDER_REVIEWS_DATASET') }}
    where REVIEW_ANSWER_TIMESTAMP is not null
    and REVIEW_CREATION_DATE is not null
),

result as (
    select
        case
            when datediff('day',
                r.REVIEW_CREATION_DATE::timestamp,
                r.REVIEW_ANSWER_TIMESTAMP::timestamp) = 0   then 'same day'
            when datediff('day',
                r.REVIEW_CREATION_DATE::timestamp,
                r.REVIEW_ANSWER_TIMESTAMP::timestamp) <= 3  then '1-3 days'
            when datediff('day',
                r.REVIEW_CREATION_DATE::timestamp,
                r.REVIEW_ANSWER_TIMESTAMP::timestamp) <= 7  then '4-7 days'
            else '7+ days'
        end                                                     as RESPONSE_TIME,
        count(distinct r.REVIEW_ID)                             as TOTAL_REVIEWS,
        round(avg(r.REVIEW_SCORE::float), 2)                    as AVG_SCORE,
        round(count(distinct case when r.REVIEW_SCORE >= 4
              then r.REVIEW_ID end) * 100.0
              / nullif(count(distinct r.REVIEW_ID), 0), 2)      as POSITIVE_RATE_PCT
    from reviews r
    inner join orders o on r.ORDER_ID = o.ORDER_ID
    group by 1
    order by AVG_SCORE desc
)

select * from result

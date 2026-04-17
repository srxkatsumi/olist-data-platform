-- 17_delay_vs_review_score.sql
-- Question: Is there a correlation between delivery delay and review score?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
    and ORDER_DELIVERED_AT is not null
    and ORDER_ESTIMATED_AT is not null
),

reviews as (
    select * from {{ source('raw', 'OLIST_ORDER_REVIEWS_DATASET') }}
),

result as (
    select
        case
            when datediff('day', o.ORDER_ESTIMATED_AT,
                o.ORDER_DELIVERED_AT) <= -7  then 'very early (7+ days before)'
            when datediff('day', o.ORDER_ESTIMATED_AT,
                o.ORDER_DELIVERED_AT) <= -1  then 'early (1-6 days before)'
            when datediff('day', o.ORDER_ESTIMATED_AT,
                o.ORDER_DELIVERED_AT) = 0    then 'on time'
            when datediff('day', o.ORDER_ESTIMATED_AT,
                o.ORDER_DELIVERED_AT) <= 7   then 'late (1-7 days)'
            else 'very late (7+ days)'
        end                                                         as DELIVERY_STATUS,
        count(distinct o.ORDER_ID)                                  as TOTAL_ORDERS,
        round(avg(r.REVIEW_SCORE::float), 2)                        as AVG_REVIEW_SCORE,
        round(min(r.REVIEW_SCORE::float), 2)                        as MIN_SCORE,
        round(max(r.REVIEW_SCORE::float), 2)                        as MAX_SCORE
    from orders o
    inner join reviews r on o.ORDER_ID = r.ORDER_ID
    group by 1
    order by AVG_REVIEW_SCORE desc
)

select * from result

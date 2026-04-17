-- 27_peak_hours_by_weekday.sql
-- Question: What are the peak purchase hours by day of the week?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

result as (
    select
        dayofweek(ORDER_PURCHASED_AT)                           as DAY_NUMBER,
        dayname(ORDER_PURCHASED_AT)                             as DAY_NAME,
        hour(ORDER_PURCHASED_AT)                                as HOUR_OF_DAY,
        count(distinct ORDER_ID)                                as TOTAL_ORDERS,
        round(count(distinct ORDER_ID) * 100.0
            / sum(count(distinct ORDER_ID))
              over (partition by dayofweek(ORDER_PURCHASED_AT)), 2) as HOUR_SHARE_PCT
    from orders
    group by 1, 2, 3
    order by TOTAL_ORDERS desc
)

select * from result

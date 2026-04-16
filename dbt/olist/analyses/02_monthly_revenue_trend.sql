-- 02_monthly_revenue_trend.sql
-- Question: What is the monthly and yearly revenue trend?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

result as (
    select
        year(o.ORDER_PURCHASED_AT)                    as YEAR,
        month(o.ORDER_PURCHASED_AT)                   as MONTH,
        date_trunc('month', o.ORDER_PURCHASED_AT)     as MONTH_DATE,
        count(distinct o.ORDER_ID)                    as TOTAL_ORDERS,
        round(sum(p.PAYMENT_VALUE::float), 2)         as TOTAL_REVENUE,
        round(avg(p.PAYMENT_VALUE::float), 2)         as AVG_ORDER_VALUE,
        round(sum(p.PAYMENT_VALUE::float)
            - lag(sum(p.PAYMENT_VALUE::float))
              over (order by date_trunc('month', o.ORDER_PURCHASED_AT)), 2
        )                                             as REVENUE_DIFF_PREV_MONTH
    from orders o
    inner join payments p on o.ORDER_ID = p.ORDER_ID
    group by 1, 2, 3
    order by 3
)

select * from result

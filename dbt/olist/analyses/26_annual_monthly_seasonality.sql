-- 26_annual_monthly_seasonality.sql
-- Question: What are the annual and monthly sales seasonality patterns?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

result as (
    select
        year(o.ORDER_PURCHASED_AT)                              as YEAR,
        month(o.ORDER_PURCHASED_AT)                             as MONTH,
        monthname(o.ORDER_PURCHASED_AT)                         as MONTH_NAME,
        count(distinct o.ORDER_ID)                              as TOTAL_ORDERS,
        round(sum(p.PAYMENT_VALUE::float), 2)                   as TOTAL_REVENUE,
        round(avg(p.PAYMENT_VALUE::float), 2)                   as AVG_ORDER_VALUE,
        round(sum(p.PAYMENT_VALUE::float) * 100.0
            / sum(sum(p.PAYMENT_VALUE::float))
              over (partition by year(o.ORDER_PURCHASED_AT)), 2) as MONTHLY_SHARE_PCT
    from orders o
    inner join payments p on o.ORDER_ID = p.ORDER_ID
    group by 1, 2, 3
    order by 1, 2
)

select * from result

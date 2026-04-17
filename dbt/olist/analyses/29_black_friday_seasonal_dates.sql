-- 29_black_friday_seasonal_dates.sql
-- Question: How do Black Friday and seasonal dates perform vs regular days?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

daily as (
    select
        date_trunc('day', o.ORDER_PURCHASED_AT)         as ORDER_DATE,
        count(distinct o.ORDER_ID)                      as TOTAL_ORDERS,
        round(sum(p.PAYMENT_VALUE::float), 2)           as TOTAL_REVENUE
    from orders o
    inner join payments p on o.ORDER_ID = p.ORDER_ID
    group by 1
),

result as (
    select
        ORDER_DATE,
        TOTAL_ORDERS,
        TOTAL_REVENUE,
        case
            when to_char(ORDER_DATE, 'MM-DD') = '11-24' then 'Black Friday 2017'
            when to_char(ORDER_DATE, 'MM-DD') = '11-23' then 'Black Friday 2018'
            when to_char(ORDER_DATE, 'MM-DD') in ('12-24','12-25') then 'Christmas'
            when to_char(ORDER_DATE, 'MM-DD') in ('01-01','01-02') then 'New Year'
            when to_char(ORDER_DATE, 'MM-DD') in ('02-12','02-13','02-14') then 'Valentines'
            when to_char(ORDER_DATE, 'MM-DD') in ('05-13','05-14') then 'Mothers Day'
            else 'regular day'
        end                                             as DATE_TYPE
    from daily
    order by TOTAL_ORDERS desc
    limit 30
)

select * from result

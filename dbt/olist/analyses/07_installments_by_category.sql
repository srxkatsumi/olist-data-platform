-- 07_installments_by_category.sql
-- Question: What is the average number of installments per category?

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS = 'delivered'
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
    where PAYMENT_TYPE = 'credit_card'
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

result as (
    select
        coalesce(t.PRODUCT_CATEGORY_NAME_ENGLISH, pr.PRODUCT_CATEGORY_NAME, 'unknown') as CATEGORY,
        count(distinct o.ORDER_ID)                          as TOTAL_ORDERS,
        round(avg(p.PAYMENT_INSTALLMENTS::float), 1)        as AVG_INSTALLMENTS,
        round(avg(p.PAYMENT_VALUE::float), 2)               as AVG_ORDER_VALUE,
        max(p.PAYMENT_INSTALLMENTS)                         as MAX_INSTALLMENTS
    from orders o
    inner join payments p   on o.ORDER_ID = p.ORDER_ID
    inner join items i      on o.ORDER_ID = i.ORDER_ID
    inner join products pr  on i.PRODUCT_ID = pr.PRODUCT_ID
    left join translation t on pr.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    group by 1
    having count(distinct o.ORDER_ID) >= 100
    order by AVG_INSTALLMENTS desc
    limit 15
)

select * from result

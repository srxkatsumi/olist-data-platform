-- mart_fraud_signals.sql
-- Question: Which customers and orders show signs of fraudulent behavior?
{{ config(materialized='table') }}

with customers as (
    select * from {{ source('raw', 'DIM_CUSTOMERS_EXTENDED') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

-- Rule 1: Same credit card token used by multiple customers
shared_card as (
    select
        CREDIT_CARD_TOKEN,
        count(distinct CUSTOMER_ID)         as CUSTOMERS_SHARING_CARD,
        count(distinct FULL_NAME)           as DIFFERENT_NAMES,
        count(distinct CUSTOMER_STATE)      as DIFFERENT_STATES,
        count(distinct CUSTOMER_CITY)       as DIFFERENT_CITIES,
        listagg(distinct FULL_NAME, ' | ')
            within group (order by FULL_NAME) as NAMES_LIST
    from customers
    where CREDIT_CARD_TOKEN is not null
    group by CREDIT_CARD_TOKEN
    having count(distinct CUSTOMER_ID) > 1
),

-- Rule 2: Orders with negative payment value
negative_payments as (
    select
        p.ORDER_ID,
        p.PAYMENT_VALUE,
        p.PAYMENT_TYPE,
        'negative_payment_value'            as FRAUD_TYPE,
        'high'                              as RISK_LEVEL
    from payments p
    where p.PAYMENT_VALUE < 0
),

-- Rule 3: Orders delivered before purchase date
invalid_dates as (
    select
        ORDER_ID,
        ORDER_PURCHASED_AT,
        ORDER_DELIVERED_AT,
        'delivery_before_purchase'          as FRAUD_TYPE,
        'high'                              as RISK_LEVEL
    from orders
    where ORDER_DELIVERED_AT is not null
      and ORDER_PURCHASED_AT is not null
      and ORDER_DELIVERED_AT < ORDER_PURCHASED_AT
),

-- Rule 4: Customers flagged for shared card
fraud_customers as (
    select
        c.CUSTOMER_ID,
        c.FULL_NAME,
        c.EMAIL,
        c.CREDIT_CARD_TOKEN,
        c.CREDIT_CARD_LAST4,
        c.CUSTOMER_STATE,
        c.CUSTOMER_CITY,
        c.IS_SYNTHETIC,
        c.DATA_SOURCE,
        sc.CUSTOMERS_SHARING_CARD,
        sc.DIFFERENT_NAMES,
        sc.DIFFERENT_STATES,
        sc.NAMES_LIST,
        case
            when sc.CUSTOMERS_SHARING_CARD >= 10 then 'critical'
            when sc.CUSTOMERS_SHARING_CARD >= 5  then 'high'
            when sc.CUSTOMERS_SHARING_CARD >= 2  then 'medium'
            else 'low'
        end                                 as RISK_LEVEL,
        'shared_credit_card'                as FRAUD_TYPE
    from customers c
    inner join shared_card sc
        on c.CREDIT_CARD_TOKEN = sc.CREDIT_CARD_TOKEN
),

-- Final result combining all fraud signals
result as (
    select
        fc.CUSTOMER_ID,
        fc.FULL_NAME,
        fc.EMAIL,
        fc.CREDIT_CARD_TOKEN,
        fc.CREDIT_CARD_LAST4,
        fc.CUSTOMER_STATE,
        fc.CUSTOMER_CITY,
        fc.FRAUD_TYPE,
        fc.RISK_LEVEL,
        fc.CUSTOMERS_SHARING_CARD,
        fc.DIFFERENT_NAMES,
        fc.NAMES_LIST,
        fc.IS_SYNTHETIC,
        fc.DATA_SOURCE,
        current_timestamp()                 as DETECTED_AT
    from fraud_customers fc
)

select * from result

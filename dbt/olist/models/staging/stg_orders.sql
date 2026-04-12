-- stg_orders.sql
-- Executed by: dbt
-- Stored in: Snowflake (STAGING schema)

with source as (
    select * from {{ source('raw', 'OLIST_ORDERS_DATASET') }}
),

staging as (
    select
        -- Identifiers
        ORDER_ID,
        CUSTOMER_ID,

        -- Order status
        ORDER_STATUS,

        -- Timestamps
        to_timestamp(ORDER_PURCHASE_TIMESTAMP) as ORDER_PURCHASED_AT,
        to_timestamp(ORDER_APPROVED_AT)        as ORDER_APPROVED_AT,
        to_timestamp(ORDER_DELIVERED_CARRIER_DATE) as ORDER_SHIPPED_AT,
        to_timestamp(ORDER_DELIVERED_CUSTOMER_DATE) as ORDER_DELIVERED_AT,
        to_timestamp(ORDER_ESTIMATED_DELIVERY_DATE) as ORDER_ESTIMATED_AT,

        -- Derived fields for analysis
        date_trunc('month', to_timestamp(ORDER_PURCHASE_TIMESTAMP)) as ORDER_MONTH,
        date_trunc('hour', to_timestamp(ORDER_PURCHASE_TIMESTAMP))  as ORDER_HOUR,
        dayofweek(to_timestamp(ORDER_PURCHASE_TIMESTAMP))           as ORDER_DAY_OF_WEEK,
        hour(to_timestamp(ORDER_PURCHASE_TIMESTAMP))                as ORDER_HOUR_OF_DAY

    from source
)

select * from staging

-- stg_orders.sql
-- Quem executa: dbt
-- Quem armazena: Snowflake (schema STAGING)

with source as (
    select * from {{ source('raw', 'OLIST_ORDERS_DATASET') }}
),

staging as (
    select
        -- Identificadores
        ORDER_ID,
        CUSTOMER_ID,

        -- Status do pedido
        ORDER_STATUS,

        -- Datas
        to_timestamp(ORDER_PURCHASE_TIMESTAMP) as ORDER_PURCHASED_AT,
        to_timestamp(ORDER_APPROVED_AT)        as ORDER_APPROVED_AT,
        to_timestamp(ORDER_DELIVERED_CARRIER_DATE) as ORDER_SHIPPED_AT,
        to_timestamp(ORDER_DELIVERED_CUSTOMER_DATE) as ORDER_DELIVERED_AT,
        to_timestamp(ORDER_ESTIMATED_DELIVERY_DATE) as ORDER_ESTIMATED_AT,

        -- Campos derivados para análise
        date_trunc('month', to_timestamp(ORDER_PURCHASE_TIMESTAMP)) as ORDER_MONTH,
        date_trunc('hour', to_timestamp(ORDER_PURCHASE_TIMESTAMP))  as ORDER_HOUR,
        dayofweek(to_timestamp(ORDER_PURCHASE_TIMESTAMP))           as ORDER_DAY_OF_WEEK,
        hour(to_timestamp(ORDER_PURCHASE_TIMESTAMP))                as ORDER_HOUR_OF_DAY

    from source
)

select * from staging

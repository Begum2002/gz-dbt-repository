-- models/intermediate/int_orders_operational.sql

WITH orders_margin AS (
    SELECT *
    FROM {{ ref('int_orders_margin') }}
),

shipping AS (
    SELECT
        orders_id,
        CAST(shipping_fee AS FLOAT64) AS shipping_fee,
        CAST(logCost AS FLOAT64) AS log_cost,
        CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM {{ ref('stg_raw__ship') }}
)

SELECT
    m.orders_id,
    ANY_VALUE(m.date_date) AS date_date,
    SUM(m.margin + s.shipping_fee - s.log_cost - s.ship_cost) AS operational_margin
FROM orders_margin AS m
LEFT JOIN shipping AS s
    ON m.orders_id = s.orders_id
GROUP BY m.orders_id

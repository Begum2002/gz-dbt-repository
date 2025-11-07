-- models/mart/finance_days.sql

WITH orders_margin AS (
    SELECT *
    FROM {{ ref('int_orders_margin') }}
),

orders_operational AS (
    SELECT *
    FROM {{ ref('int_orders_operational') }}
),

shipping AS (
    SELECT
        orders_id,
        CAST(shipping_fee AS FLOAT64) AS shipping_fee,
        CAST(logCost AS FLOAT64) AS logistics_cost,
        CAST(ship_cost AS FLOAT64) AS shipping_cost
    FROM {{ ref('stg_raw__ship') }}
)

SELECT
    m.date_date AS order_date,
    COUNT(DISTINCT m.orders_id) AS total_orders,
    SUM(m.turnover) AS total_revenue,
    SAFE_DIVIDE(SUM(m.turnover), COUNT(DISTINCT m.orders_id)) AS avg_basket_value,
    SUM(o.operational_margin) AS total_operational_margin,
    SUM(m.purchase_cost) AS total_purchase_cost,
    SUM(s.shipping_fee) AS total_shipping_fee,
    SUM(s.logistics_cost) AS total_logistics_cost,
    SUM(m.qty) AS total_quantity
FROM orders_margin AS m
LEFT JOIN orders_operational AS o
    ON m.orders_id = o.orders_id
LEFT JOIN shipping AS s
    ON m.orders_id = s.orders_id
GROUP BY m.date_date
ORDER BY m.date_date

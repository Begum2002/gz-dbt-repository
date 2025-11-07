WITH sales AS (
    SELECT *
    FROM {{ ref('stg_raw__sales') }}
),
products AS (
    SELECT *
    FROM {{ ref('stg_raw__product') }}
)
SELECT
    s.orders_id,
    s.products_id,
    s.date_date,
    s.revenue AS turnover,
    s.quantity AS qty,
    p.purchase_price,
    CAST(s.quantity * p.purchase_price AS FLOAT64) AS purchase_cost,
    CAST(s.revenue - (s.quantity * p.purchase_price) AS FLOAT64) AS margin
FROM sales s
LEFT JOIN products p
    ON s.products_id = p.products_id

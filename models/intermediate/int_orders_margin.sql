-- models/intermediate/int_orders_margin.sql

SELECT
    orders_id,
    ANY_VALUE(date_date) AS date_date,
    SUM(turnover) AS turnover,
    SUM(qty) AS qty,
    SUM(purchase_cost) AS purchase_cost,
    SUM(margin) AS margin
FROM {{ ref('int_sales_margin') }}
GROUP BY orders_id

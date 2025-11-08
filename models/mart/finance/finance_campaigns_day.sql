WITH finance AS (
    SELECT * FROM {{ ref('finance_days') }}
),
campaigns AS (
    SELECT * FROM {{ ref('int_campaigns_day') }}
)
SELECT
    f.order_date AS date,
    (f.total_operational_margin - c.ads_cost) AS ads_margin,
    f.avg_basket_value AS average_basket,
    f.total_operational_margin AS operational_margin,
    c.ads_cost,
    c.ads_impression,
    c.ads_clicks,
    f.total_quantity AS quantity,
    f.total_revenue AS revenue,
    f.total_purchase_cost AS purchase_cost,
    f.total_operational_margin AS margin,
    f.total_shipping_fee AS shipping_fee,
    f.total_logistics_cost AS log_cost,
    f.total_shipping_fee AS ship_cost
FROM finance f
LEFT JOIN campaigns c
    ON f.order_date = c.date_date
ORDER BY f.order_date DESC

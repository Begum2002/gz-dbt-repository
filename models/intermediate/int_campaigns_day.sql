WITH campaigns AS (
    SELECT * FROM {{ ref('int_campaigns') }}
)
SELECT
    date_date,
    SUM(ads_cost) AS ads_cost,
    SUM(ads_clicks) AS ads_clicks,
    SUM(ads_impression) AS ads_impression
FROM campaigns
GROUP BY date_date
ORDER BY date_date DESC;

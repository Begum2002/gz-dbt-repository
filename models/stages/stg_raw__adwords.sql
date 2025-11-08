WITH source AS (
    SELECT * FROM {{ source('raw', 'adwords') }}
),

renamed AS (
    SELECT
        date_date,
        paid_source,
        campaign_key,
        camPGN_name AS campaign_name,  -- yeniden adlandırma
        CAST(ads_cost AS FLOAT64) AS ads_cost,  -- tip dönüşümü (string → float)
        impression AS ads_impression,
        click AS ads_clicks
    FROM source
)

SELECT * FROM renamed

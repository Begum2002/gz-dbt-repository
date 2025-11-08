WITH source_adwords AS (
    SELECT * FROM {{ source('raw', 'adwords') }}
),
source_bing AS (
    SELECT * FROM {{ source('raw', 'bing') }}
),
source_criteo AS (
    SELECT * FROM {{ source('raw', 'criteo') }}
),
source_facebook AS (
    SELECT * FROM {{ source('raw', 'facebook') }}
),

-- Tüm kaynakları birleştiriyoruz
unioned AS (
    SELECT * FROM source_adwords
    UNION ALL
    SELECT * FROM source_bing
    UNION ALL
    SELECT * FROM source_criteo
    UNION ALL
    SELECT * FROM source_facebook
),

-- Kolon isimlerini standart hale getiriyoruz
renamed AS (
    SELECT
        date_date,
        paid_source,              --  Burada "facebook ham verilerinde paid_source" değiştiğini söylemişlerdi
        campaign_key,
        camPGN_name,
        CAST(ads_cost AS FLOAT64) AS ads_cost,
        CAST(impression AS FLOAT64) AS ads_impression,
        CAST(click AS FLOAT64) AS ads_clicks
    FROM unioned
)

SELECT * FROM renamed

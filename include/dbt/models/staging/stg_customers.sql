
WITH customers_cte AS (

    SELECT DISTINCT
        author_id,
        lower(trim(skin_tone)) AS skin_tone,
        lower(trim(eye_color)) AS eye_color,
        lower(trim(skin_type)) AS skin_type,
        lower(trim(hair_color)) AS hair_color,
        ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY skin_type) AS rn
    FROM {{ source('skincare_processed', 'reviews') }}
    WHERE author_id IS NOT NULL 
      AND skin_type IS NOT NULL
       AND skin_type IN ('normal', 'dry', 'oily', 'combination')
)

SELECT 
    author_id,
    skin_tone,
    eye_color,
    skin_type,
    hair_color,
FROM customers_cte
WHERE rn = 1

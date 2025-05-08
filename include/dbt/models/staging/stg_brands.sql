WITH brands_cte AS (

    SELECT  DISTINCT
        brand_id,
        lower(trim(brand_name)) as brand_name
    FROM {{ source('skincare_processed', 'products') }}
    WHERE brand_id IS NOT NULL
)

SELECT 
    brand_id,
    brand_name
FROM brands_cte

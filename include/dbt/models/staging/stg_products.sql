
WITH products_cte AS (

    SELECT DISTINCT
        product_id,
        lower(trim(product_name)) AS product_name,
        brand_id,
        lower(trim(brand_name)) as brand_name,
        loves_count,
        average_rating,
        reviews AS review_count,
        size AS product_size,
        lower(trim(variation_type)) AS variation_type,
        variation_value AS variation_value,
        ingredients,
        cast(price_usd AS float64) AS price_usd,
        highlights,
        lower(trim(primary_category)) AS primary_category,
        lower(trim(secondary_category)) AS secondary_category,
        lower(trim(tertiary_category)) AS tertiary_category
    
    FROM {{ source('skincare_processed', 'products') }}
    WHERE product_id IS NOT NULL 
      AND price_usd IS NOT NULL
)

SELECT
    product_id,
    product_name,
    brand_id,
    brand_name,
    loves_count,
    average_rating,
    review_count,
    product_size,
    variation_type,
    variation_value,
    ingredients,
    price_usd,
    highlights,
    primary_category,
    secondary_category,
    tertiary_category
FROM products_cte

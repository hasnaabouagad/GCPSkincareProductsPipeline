SELECT
    MD5(product_id) as product_key,
    product_id,
    product_name,
    brand_id,
    brand_name,
    product_size,
    variation_type,
    variation_value,
    ingredients,
    highlights,
    primary_category,
    secondary_category,
    tertiary_category
FROM {{ ref('stg_products') }}

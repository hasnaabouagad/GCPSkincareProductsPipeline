SELECT
    p.brand_name,
    AVG(f.rating) as avg_brand_rating
FROM {{ ref('fact_care') }} f
JOIN {{ ref('dim_products') }} p ON f.product_key = p.product_key
GROUP BY p.brand_name
ORDER BY avg_brand_rating DESC


SELECT
    p.product_name,
    AVG(f.rating) as avg_rating,
    AVG(f.price_usd) as avg_price
FROM {{ ref('fact_care') }} f
JOIN {{ ref('dim_products') }} p ON f.product_key = p.product_key
GROUP BY p.product_name

SELECT
    p.product_name,
    c.skin_type,
    AVG(f.rating) AS avg_rating,
    AVG(f.price_usd) AS avg_price
FROM {{ ref('fact_care') }} f
JOIN {{ ref('dim_products') }} p ON f.product_key = p.product_key
JOIN {{ ref('dim_customers') }} c ON f.customer_key = c.customer_key
GROUP BY p.product_name, c.skin_type
ORDER BY avg_rating DESC, avg_price ASC

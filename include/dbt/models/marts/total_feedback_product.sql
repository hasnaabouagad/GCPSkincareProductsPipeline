SELECT
    p.product_name,
    SUM(f.total_feedback_count) AS total_feedback_count,
    SUM(f.total_pos_feedback_count) AS total_pos_feedback_count,
    SUM(f.total_neg_feedback_count) AS total_neg_feedback_count
FROM {{ ref('fact_care') }} AS f
LEFT JOIN {{ ref('dim_products') }} AS p
    ON f.product_key = p.product_key
GROUP BY p.product_name

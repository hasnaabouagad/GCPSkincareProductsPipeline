SELECT
    MD5(CAST(r.review_id AS STRING)) as fact_key,
    MD5(r.author_id) AS customer_key,
    MD5(r.product_id) AS product_key,
    d.date_key, 
    r.review_id,
    r.rating,
    p.loves_count,
    p.review_count,
    p.price_usd,
    r.total_feedback_count,
    r.total_neg_feedback_count,
    r.total_pos_feedback_count,
FROM {{ ref('stg_reviews') }} r 
JOIN {{ ref('stg_products') }} p ON r.product_id = p.product_id
JOIN {{ ref('stg_customers') }} c  ON r.author_id = c.author_id
JOIN {{ ref('dim_date') }} d ON r.submission_time = d.date_key

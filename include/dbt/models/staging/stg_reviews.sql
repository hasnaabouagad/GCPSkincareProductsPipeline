
WITH reviews_cte AS (

    SELECT DISTINCT
        review_id,
        author_id,
        product_id,
        cast(rating AS integer) AS rating,
        cast(is_recommended AS float64) AS is_recommended,
        helpfulness,
        total_feedback_count,
        total_neg_feedback_count,
        total_pos_feedback_count,
        submission_time,
        trim(review_text) AS review_text,
        trim(review_title) AS review_title
    FROM {{ source('skincare_processed', 'reviews') }}
    WHERE review_id IS NOT NULL
)

SELECT
    review_id,
    author_id,
    product_id,
    rating,
    is_recommended,
    helpfulness,
    total_feedback_count,
    total_neg_feedback_count,
    total_pos_feedback_count,
    submission_time,
    review_text,
    review_title
FROM reviews_cte

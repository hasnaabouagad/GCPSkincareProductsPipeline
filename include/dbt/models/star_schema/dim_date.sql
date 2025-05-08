SELECT
    date_key,
    EXTRACT(DAYOFWEEK FROM date_key) AS day_of_week,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(QUARTER FROM date_key) AS quarter,
    EXTRACT(YEAR FROM date_key) AS year
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '2017-01-01', DATE '2025-01-01', INTERVAL 1 DAY)
) AS date_key


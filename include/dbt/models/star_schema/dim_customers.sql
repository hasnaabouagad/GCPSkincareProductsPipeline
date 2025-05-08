SELECT 
    MD5(author_id) as customer_key,
    author_id,
    skin_tone,
    eye_color,
    skin_type,
    hair_color,
FROM {{ ref('stg_customers') }}

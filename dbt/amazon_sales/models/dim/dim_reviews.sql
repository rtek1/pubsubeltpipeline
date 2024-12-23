-- models/dim_reviews.sql
SELECT
    review_id,
    review_title,
    review_content
FROM
    {{ ref('stg_amazon_sales_data') }}
GROUP BY
    review_id, review_title, review_content
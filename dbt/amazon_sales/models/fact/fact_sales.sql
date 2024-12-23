-- models/fact/fact_sales.sql
SELECT
    f.product_id,
    f.user_id,
    f.discounted_price,
    f.actual_price,
    f.discount_percentage,
    f.rating,
    f.rating_count,
    f.review_id,
    p.category,
    u.user_name,
    r.review_title
FROM
    {{ ref('stg_amazon_sales_data') }} AS f
LEFT JOIN {{ ref('dim_products') }} AS p
    ON f.product_id = p.product_id
LEFT JOIN {{ ref('dim_users') }} AS u
    ON f.user_id = u.user_id
LEFT JOIN {{ ref('dim_reviews') }} AS r
    ON f.review_id = r.review_id

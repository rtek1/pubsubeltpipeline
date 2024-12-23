-- models/mart/category_sales_performance.sql
SELECT
    p.category,
    SUM(
        SAFE_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(f.actual_price, '₹', ''), ',', ''), '−', '-'), ' ', '') AS FLOAT64
        )
    ) AS revenue_generated,
    AVG(SAFE_CAST(f.rating AS FLOAT64)) AS average_rating
FROM
    {{ ref('fact_sales') }} AS f
LEFT JOIN
    {{ ref('dim_products') }} AS p
    ON f.product_id = p.product_id
GROUP BY
    p.category
ORDER BY
    revenue_generated DESC

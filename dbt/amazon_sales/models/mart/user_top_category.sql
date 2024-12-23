-- models/mart/user_top_category.sql
WITH category_revenue_per_user AS (
    SELECT
        f.user_id,
        u.simple_user_name,  -- Include simple_user_name from dim_users
        p.category,
        SUM(
        SAFE_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(f.actual_price, '₹', ''), ',', ''), '−', '-'), ' ', '') AS FLOAT64
        )
        ) AS total_revenue,
        COUNT(f.product_id) AS order_count
    FROM
        {{ ref('fact_sales') }} AS f
    LEFT JOIN
        {{ ref('dim_products') }} AS p
        ON f.product_id = p.product_id
    LEFT JOIN
        {{ ref('dim_users') }} AS u
        ON f.user_id = u.user_id  -- Join dim_users for simple_user_name
    GROUP BY
        f.user_id, u.simple_user_name, p.category  -- Include all non-aggregated columns
),
ranked_categories AS (
    SELECT
        user_id,
        simple_user_name,
        category,
        total_revenue,
        order_count,
        RANK() OVER (PARTITION BY user_id ORDER BY total_revenue DESC) AS category_rank
    FROM
        category_revenue_per_user
)
SELECT
    user_id,
    simple_user_name,
    category AS top_category,
    total_revenue,
    order_count
FROM
    ranked_categories
WHERE
    category_rank = 1
ORDER BY
    total_revenue DESC

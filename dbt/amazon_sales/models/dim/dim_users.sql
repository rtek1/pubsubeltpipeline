-- models/dim/dim_users.sql
SELECT
    user_id,
    user_name,
    SPLIT(user_name, ',')[SAFE_OFFSET(0)] AS simple_user_name
FROM
    {{ ref('stg_amazon_sales_data') }}
GROUP BY
    user_id, user_name
-- models/stg/stg_amazon_sales_data.sql
SELECT
    *
FROM
    {{ source('amazonsales', 'amazon_sales_data') }}  -- Use source() instead of direct reference
WHERE
    product_id IS NOT NULL
    AND product_name IS NOT NULL
    AND category IS NOT NULL
    AND actual_price IS NOT NULL
    AND user_id IS NOT NULL

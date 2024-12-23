-- models/dim/dim_products.sql

WITH split_categories AS (
    SELECT
        product_id,
        product_name,
        about_product,
        img_link,
        product_link,
        category,
        -- Split the category string into an array using the "|" delimiter
        SPLIT(category, '|') AS subcategories
    FROM
        {{ ref('stg_amazon_sales_data') }}
),

categorized_products AS (
    SELECT
        product_id,
        product_name,
        about_product,
        img_link,
        product_link,
        -- Keep the first part of the split as the main category
        subcategories[OFFSET(0)] AS category,
        -- Dynamically extract other parts of the array
        IF(ARRAY_LENGTH(subcategories) > 1, subcategories[OFFSET(1)], NULL) AS subcat1,
        IF(ARRAY_LENGTH(subcategories) > 2, subcategories[OFFSET(2)], NULL) AS subcat2,
        IF(ARRAY_LENGTH(subcategories) > 3, subcategories[OFFSET(3)], NULL) AS subcat3,
        IF(ARRAY_LENGTH(subcategories) > 4, subcategories[OFFSET(4)], NULL) AS subcat4,
        IF(ARRAY_LENGTH(subcategories) > 5, subcategories[OFFSET(5)], NULL) AS subcat5,
        IF(ARRAY_LENGTH(subcategories) > 6, subcategories[OFFSET(6)], NULL) AS subcat6,
        IF(ARRAY_LENGTH(subcategories) > 7, subcategories[OFFSET(6)], NULL) AS subcat7
        -- Add more subcategories as needed by extending this pattern
    FROM
        split_categories
)

SELECT * 
FROM categorized_products
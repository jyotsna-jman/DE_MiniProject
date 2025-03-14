WITH product_purchases AS (
    SELECT 
        customer_id, 
        product_id, 
        COUNT(*) AS purchase_count
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id, product_id
)
 
SELECT
    customer_id,
    COUNT(DISTINCT product_id) AS total_products_purchased,
    SUM(CASE WHEN purchase_count = 1 THEN 1 ELSE 0 END) AS churned_products
FROM product_purchases
GROUP BY customer_id
ORDER BY total_products_purchased DESC
LIMIT 10
WITH new_logos AS (
    SELECT
        t.customer_id,
        YEAR(t.payment_date) AS fiscal_year  
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_customers') }} c ON t.customer_id = c.customer_id  
    GROUP BY t.customer_id, fiscal_year
)
 
SELECT
    fiscal_year,
    COUNT(DISTINCT customer_id) AS new_logos
FROM new_logos
GROUP BY fiscal_year
ORDER BY fiscal_year DESC
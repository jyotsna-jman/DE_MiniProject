SELECT
    t.customer_id,
    c.customer_name,
    ROUND(SUM(t.revenue), 2) AS total_revenue,
    DENSE_RANK() OVER (ORDER BY ROUND(SUM(t.revenue), 2) DESC) AS customer_revenue_rank
FROM {{ ref('stg_transactions') }} t 
JOIN {{ ref('stg_customers') }} c 
ON t.customer_id = c.customer_id
GROUP BY t.customer_id,c.customer_name
ORDER BY total_revenue DESC


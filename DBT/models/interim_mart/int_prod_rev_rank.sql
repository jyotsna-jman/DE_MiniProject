SELECT
    t.product_id,
    p.product_family,
    p.product_sub_family,
    ROUND(SUM(t.revenue), 2) AS total_revenue,
    DENSE_RANK() OVER (ORDER BY ROUND(SUM(t.revenue), 2) DESC) AS product_revenue_rank,
FROM {{ ref('stg_transactions') }} t
JOIN {{ ref('stg_products') }} p ON t.product_id = p.product_id
GROUP BY t.product_id, p.product_family, p.product_sub_family
ORDER BY total_revenue DESC


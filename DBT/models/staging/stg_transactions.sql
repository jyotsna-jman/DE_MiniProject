WITH cleaned_transaction_data AS (
    SELECT
        CAST("customer_id" AS STRING)             AS customer_id,
        TRIM(UPPER("product_id"))                 AS product_id,
        TO_DATE("payment_month",'MM/DD/YYYY')   AS payment_date,
        COALESCE(CAST("revenue" AS FLOAT), 0)     AS revenue,
        COALESCE(CAST("quantity" AS INTEGER), 0)  AS quantity,
        CAST("revenue_type" AS INTEGER)           AS revenue_type,
        TRIM(UPPER("companies"))                  AS company
    FROM {{ source('project_source', 'TRANSACTIONS') }}
    WHERE customer_id IS NOT NULL 
        AND product_id IS NOT NULL
)


SELECT *
FROM
  cleaned_transaction_data
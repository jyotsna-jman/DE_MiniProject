WITH cleaned_customers_data AS (
    SELECT
        CAST("customer_id" AS STRING)               AS customer_id,
        COALESCE(TRIM("customername"),'Unknown')    AS customer_name,
        TRIM(UPPER("company"))                      AS company_name
    FROM {{ source('project_source', 'CUSTOMERS') }}
    WHERE "customer_id"    IS NOT NULL
      AND "customername"   IS NOT NULL
      AND "company"        IS NOT NULL
)

SELECT
    customer_id,
    customer_name,
    company_name
FROM
    cleaned_customers_data
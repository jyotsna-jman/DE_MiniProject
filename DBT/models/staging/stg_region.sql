WITH cleaned_region_data AS (
    SELECT
        CAST("customer_id" AS STRING)  AS customer_id,
        TRIM(UPPER("country"))             AS country,
        TRIM(UPPER("region"))              AS region
    FROM {{ source('project_source', 'COUNTRY_REGION') }}
    WHERE "customer_id"    IS NOT NULL
      AND "country"        IS NOT NULL
      AND "region"         IS NOT NULL
)

SELECT 
    customer_id,
    country,
    region
FROM
  cleaned_region_data
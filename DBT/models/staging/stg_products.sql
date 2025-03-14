WITH cleaned_products_data AS (
    SELECT
        TRIM(UPPER("product_id")) AS product_id,
        TRIM(UPPER("PRODUCT_Family")) AS product_family,
        TRIM(UPPER("product_sub_family")) AS product_sub_family
    FROM {{ source('project_source', 'PRODUCTS') }}
    WHERE product_id IS NOT NULL
      AND product_family IS NOT NULL
      AND product_sub_family IS NOT NULL
)

SELECT 
    product_id,
    product_family,
    product_sub_family
FROM
  cleaned_products_data




  
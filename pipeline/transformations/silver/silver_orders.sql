-- Silver: one row per order, typed and renamed, with quality flags instead of filters.
CREATE OR REFRESH MATERIALIZED VIEW silver.silver_orders (
  CONSTRAINT has_order_id EXPECT (order_id IS NOT NULL),
  CONSTRAINT discount_within_gross EXPECT (discount <= gross_amount)
)
COMMENT 'Exactly one row per order (the latest copy if an order appears more than once). Amounts in USD. Flags: is_void (cancelled), is_test_order, has_bad_values (the order id, date or amount is missing or could not be read). Gold decides which flags to filter on.'
AS SELECT
  try_cast(order_id AS BIGINT)                AS order_id,
  try_cast(order_date AS DATE)                AS order_date,
  trim(region)                                AS region,
  try_cast(customer_id AS BIGINT)             AS customer_id,
  try_cast(gross_amount AS DECIMAL(12, 2))    AS gross_amount,
  coalesce(try_cast(discount AS DECIMAL(12, 2)), 0) AS discount,
  upper(trim(status))                         AS status,
  upper(trim(status)) = 'VOID'                AS is_void,
  coalesce(lower(trim(is_test)) IN ('true', 'yes', '1'), false) AS is_test_order,
  (try_cast(order_id AS BIGINT) IS NULL
    OR try_cast(order_date AS DATE) IS NULL
    OR try_cast(gross_amount AS DECIMAL(12, 2)) IS NULL) AS has_bad_values,
  source_file,
  file_landed_at
FROM bronze.bronze_orders
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY file_landed_at DESC, source_file DESC
) = 1;

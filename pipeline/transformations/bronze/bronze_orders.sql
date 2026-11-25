-- Bronze: the orders files exactly as received. Every column lands as a string;
-- silver does the typing, so an odd value never breaks the load.
CREATE OR REFRESH MATERIALIZED VIEW bronze.bronze_orders (
  CONSTRAINT has_order_id EXPECT (order_id IS NOT NULL)
)
COMMENT 'Orders files from the landing volume, as received: one row per row in the source files, all values as strings, plus the source file and when it landed.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_landed_at
FROM read_files(
  '${landing_path}/orders/',
  format => 'csv',
  header => true,
  inferColumnTypes => false
);

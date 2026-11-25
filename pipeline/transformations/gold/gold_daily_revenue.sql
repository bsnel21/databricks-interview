-- Gold: shaped around the question "how is revenue trending by region, and is today unusual?"
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_daily_revenue
COMMENT 'One row per order date and region: order count, gross and net revenue (USD), and net revenue compared with that region''s average day. Counts only valid orders (not void, not test, no bad values).'
AS
WITH daily AS (
  SELECT
    order_date,
    region,
    COUNT(*)                         AS order_count,
    SUM(gross_amount)                AS gross_revenue,
    SUM(gross_amount - discount)     AS net_revenue
  FROM silver.silver_orders
  WHERE NOT is_void AND NOT is_test_order AND NOT has_bad_values
  GROUP BY order_date, region
)
SELECT
  order_date,
  region,
  order_count,
  gross_revenue,
  net_revenue,
  ROUND(try_divide(net_revenue, AVG(net_revenue) OVER (PARTITION BY region)), 2) AS net_revenue_vs_region_average
FROM daily;

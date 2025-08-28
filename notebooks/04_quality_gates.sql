-- Quick data quality & reconciliation checks
USE CATALOG retail_demo;

-- ========= 1) Bronze row counts =========
SELECT 'bronze.products' AS table_name, COUNT(*) AS rows FROM bronze.products
UNION ALL
SELECT 'bronze.sales', COUNT(*) FROM bronze.sales
UNION ALL
SELECT 'bronze.return_reasons', COUNT(*) FROM bronze.return_reasons;

-- ========= 2) Silver row counts =========
SELECT 'silver.dim_date' AS table_name, COUNT(*) AS rows FROM silver.dim_date
UNION ALL
SELECT 'silver.dim_product', COUNT(*) FROM silver.dim_product
UNION ALL
SELECT 'silver.dim_return_reason', COUNT(*) FROM silver.dim_return_reason
UNION ALL
SELECT 'silver.fact_sales_clean', COUNT(*) FROM silver.fact_sales_clean;

-- ========= 3) Distinct product parity (Bronze vs Dim) =========
SELECT
  (SELECT COUNT(DISTINCT product_id) FROM bronze.products) AS bronze_distinct_products,
  (SELECT COUNT(*) FROM silver.dim_product)                AS dim_product_rows;

-- ========= 4) RI check: fact rows missing product_sk (should be 0) =========
SELECT COUNT(*) AS fact_rows_without_product_sk
FROM silver.fact_sales_clean f
WHERE f.product_sk IS NULL;

-- ========= 5) Domain checks on fact =========
SELECT
  SUM(CASE WHEN unit_price < 0 THEN 1 ELSE 0 END)               AS bad_unit_price,
  SUM(CASE WHEN discount < 0 OR discount > 1 THEN 1 ELSE 0 END) AS bad_discount,
  SUM(CASE WHEN revenue < 0 THEN 1 ELSE 0 END)                   AS bad_revenue
FROM silver.fact_sales_clean;

-- ========= 6) Date coverage (min/max) =========
SELECT MIN(date) AS min_date, MAX(date) AS max_date
FROM silver.dim_date;

-- ========= 7) Sanity sample from Gold =========
SELECT * FROM gold.monthly_sales ORDER BY year, month, category, brand LIMIT 20;

-- ========= (Optional) Performance maintenance =========
-- Run these only if you want to compact / optimize storage.
-- OPTIMIZE silver.fact_sales_clean ZORDER BY (date_key, product_sk);
-- VACUUM   silver.fact_sales_clean RETAIN 168 HOURS;  -- keep 7 days of history

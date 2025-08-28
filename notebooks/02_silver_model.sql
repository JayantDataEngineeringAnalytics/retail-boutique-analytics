-- Build SILVER dims + clean fact from BRONZE (Unity Catalog)
USE CATALOG retail_demo;

-- Safety: ensure schemas exist
CREATE SCHEMA IF NOT EXISTS silver;

-- =========================
-- dim_date
-- =========================
CREATE OR REPLACE TABLE retail_demo.silver.dim_date AS
SELECT
  CAST(date_format(purchase_ts, 'yyyyMMdd') AS INT) AS date_key,
  CAST(purchase_ts AS DATE)                   AS date,
  YEAR(purchase_ts)                           AS year,
  QUARTER(purchase_ts)                        AS quarter,
  MONTH(purchase_ts)                          AS month,
  date_format(purchase_ts, 'MMMM')            AS month_name,
  DAY(purchase_ts)                            AS day
FROM (
  SELECT DISTINCT CAST(purchase_date AS TIMESTAMP) AS purchase_ts
  FROM retail_demo.bronze.sales
)
WHERE purchase_ts IS NOT NULL;

-- =========================
-- dim_product  (Type-1 scaffold; SK via ROW_NUMBER for CE/UC portability)
-- =========================
CREATE OR REPLACE TABLE retail_demo.silver.dim_product AS
WITH base AS (
  SELECT DISTINCT
    product_id,
    INITCAP(TRIM(category))                        AS category,
    INITCAP(TRIM(brand))                           AS brand,
    INITCAP(TRIM(season))                          AS season,
    UPPER(TRIM(CAST(size AS STRING)))              AS size,
    INITCAP(TRIM(color))                           AS color
  FROM retail_demo.bronze.products
)
SELECT
  ROW_NUMBER() OVER (ORDER BY product_id)          AS product_sk,
  product_id, category, brand, season, size, color,
  current_timestamp()                              AS eff_from,
  CAST(NULL AS TIMESTAMP)                          AS eff_to,
  TRUE                                             AS is_current
FROM base;

-- =========================
-- dim_return_reason (optional)
-- =========================
CREATE OR REPLACE TABLE retail_demo.silver.dim_return_reason AS
WITH base AS (
  SELECT DISTINCT return_reason
  FROM retail_demo.bronze.return_reasons
  WHERE return_reason IS NOT NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY return_reason)       AS return_reason_sk,
  return_reason
FROM base;

-- =========================
-- fact_sales_clean
-- =========================
CREATE OR REPLACE TABLE retail_demo.silver.fact_sales_clean AS
WITH src AS (
  SELECT
    -- cast & normalize
    CAST(s.purchase_date AS TIMESTAMP)                           AS purchase_ts,
    p.product_id,
    INITCAP(TRIM(p.category))                                    AS category,
    INITCAP(TRIM(p.brand))                                       AS brand,
    INITCAP(TRIM(p.season))                                      AS season,
    UPPER(TRIM(CAST(p.size AS STRING)))                          AS size,
    INITCAP(TRIM(p.color))                                       AS color,
    CAST(s.current_price AS DOUBLE)                              AS unit_price_raw,
    CAST(s.markdown_percentage AS DOUBLE)                        AS markdown_pct_raw,
    -- robust boolean mapping
    CASE
      WHEN lower(CAST(s.is_returned AS STRING)) IN ('true','1','yes','y') THEN TRUE
      ELSE FALSE
    END                                                          AS is_returned_bool,
    CAST(s.customer_rating AS DOUBLE)                            AS customer_rating,
    CAST(s.stock_quantity AS INT)                                AS stock_quantity,
    s.return_reason
  FROM retail_demo.bronze.sales s
  JOIN retail_demo.bronze.products p USING (product_id)
)
, typed AS (
  SELECT
    purchase_ts,
    product_id, category, brand, season, size, color,
    CASE WHEN unit_price_raw < 0 OR unit_price_raw IS NULL THEN 0.0 ELSE unit_price_raw END AS unit_price,
    CASE
      WHEN markdown_pct_raw IS NULL THEN 0.0
      WHEN markdown_pct_raw < 0   THEN 0.0
      WHEN markdown_pct_raw > 100 THEN 100.0
      ELSE markdown_pct_raw
    END                                                         AS markdown_pct_clamped,
    is_returned_bool                                            AS is_returned,
    customer_rating,
    stock_quantity,
    return_reason
  FROM src
)
SELECT
  -- keys
  CAST(date_format(t.purchase_ts, 'yyyyMMdd') AS INT)           AS date_key,
  dp.product_sk,
  drr.return_reason_sk,
  -- measures / modeled columns
  1                                                             AS qty,
  t.unit_price                                                  AS unit_price,
  (t.markdown_pct_clamped / 100.0)                              AS discount,
  ROUND(t.unit_price * (1 - (t.markdown_pct_clamped / 100.0)), 2) AS net_price,
  CASE WHEN t.is_returned THEN 0.0
       ELSE ROUND(t.unit_price * (1 - (t.markdown_pct_clamped / 100.0)), 2)
  END                                                           AS revenue,
  t.customer_rating,
  t.is_returned,
  t.stock_quantity
FROM typed t
JOIN retail_demo.silver.dim_product       dp  ON dp.product_id = t.product_id AND dp.is_current = TRUE
JOIN retail_demo.silver.dim_date          dd  ON dd.date_key   = CAST(date_format(t.purchase_ts, 'yyyyMMdd') AS INT)
LEFT JOIN retail_demo.silver.dim_return_reason drr ON drr.return_reason = t.return_reason
WHERE t.purchase_ts IS NOT NULL;

-- =========================
-- quality gates (quick checks)
-- =========================
-- not-null & domains
SELECT
  SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)      AS null_date_key,
  SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END)    AS null_product_sk,
  SUM(CASE WHEN unit_price < 0 THEN 1 ELSE 0 END)        AS bad_unit_price,
  SUM(CASE WHEN discount < 0 OR discount > 1 THEN 1 ELSE 0 END) AS bad_discount,
  SUM(CASE WHEN revenue < 0 THEN 1 ELSE 0 END)           AS bad_revenue
FROM retail_demo.silver.fact_sales_clean;

-- counts
SELECT COUNT(*) AS dim_date_rows      FROM retail_demo.silver.dim_date;
SELECT COUNT(*) AS dim_product_rows   FROM retail_demo.silver.dim_product;
SELECT COUNT(*) AS fact_rows          FROM retail_demo.silver.fact_sales_clean;

-- Gold marts (all BI calcs live here)
USE CATALOG retail_demo;

-- =========================
-- monthly_sales
-- =========================
CREATE OR REPLACE VIEW retail_demo.gold.monthly_sales AS
SELECT
  d.year,
  d.month,
  d.month_name,
  p.category,
  p.brand,
  SUM(f.revenue)                                         AS total_revenue,
  SUM(f.qty)                                             AS units_sold,
  SUM(f.qty)                                             AS orders,  -- one fact row = one order
  ROUND(SUM(f.revenue) / NULLIF(SUM(f.qty), 0), 2) AS aov,
  ROUND(100.0 * SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END) / NULLIF(SUM(f.qty), 0), 2) AS return_rate_pct,
  AVG(f.customer_rating)                                 AS avg_rating
FROM retail_demo.silver.fact_sales_clean f
JOIN retail_demo.silver.dim_date d    ON d.date_key    = f.date_key
JOIN retail_demo.silver.dim_product p ON p.product_sk  = f.product_sk
GROUP BY d.year, d.month, d.month_name, p.category, p.brand;

-- =========================
-- brand_season
-- =========================
CREATE OR REPLACE VIEW retail_demo.gold.brand_season AS
SELECT
  p.brand,
  p.season,
  SUM(f.revenue)                             AS total_revenue,
  SUM(f.qty)                                 AS units_sold,
  AVG(100.0 * f.discount)                    AS avg_discount_pct,
  SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END) AS returns
FROM retail_demo.silver.fact_sales_clean f
JOIN retail_demo.silver.dim_product p ON p.product_sk = f.product_sk
GROUP BY p.brand, p.season;

-- =========================
-- top_products (Top 50 by revenue)
-- =========================
CREATE OR REPLACE VIEW retail_demo.gold.top_products AS
WITH ranked AS (
  SELECT
    p.product_id,
    p.brand,
    p.category,
    p.season,
    SUM(f.revenue) AS revenue,
    ROW_NUMBER() OVER (ORDER BY SUM(f.revenue) DESC) AS rn,
    SUM(SUM(f.revenue)) OVER () AS total_rev
  FROM retail_demo.silver.fact_sales_clean f
  JOIN retail_demo.silver.dim_product p ON p.product_sk = f.product_sk
  GROUP BY p.product_id, p.brand, p.category, p.season
)
SELECT
  product_id, brand, category, season, revenue, rn,
  ROUND(100.0 * revenue / NULLIF(total_rev, 0), 2) AS contribution_pct
FROM ranked
WHERE rn <= 50;

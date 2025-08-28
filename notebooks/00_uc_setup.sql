-- Databricks Free Edition (Unity Catalog) setup
CREATE CATALOG IF NOT EXISTS retail_demo;
USE CATALOG retail_demo;

CREATE SCHEMA IF NOT EXISTS landing;   -- for UC Volume (file uploads)
CREATE SCHEMA IF NOT EXISTS bronze;    -- entity-oriented staging
CREATE SCHEMA IF NOT EXISTS silver;    -- conformed dims/facts
CREATE SCHEMA IF NOT EXISTS gold;      -- BI marts

CREATE VOLUME IF NOT EXISTS landing.kaggle_raw;

-- After running this in Databricks:
-- Upload the CSV here:
-- /Volumes/retail_demo/landing/kaggle_raw/fashion_boutique_dataset.csv

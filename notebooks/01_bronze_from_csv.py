# Databricks notebook: CSV → BRONZE (entity-oriented, no cleansing)
from pyspark.sql import functions as F

# Use our UC catalog
spark.sql("USE CATALOG retail_demo")

# Source file we uploaded to the UC Volume
src = "/Volumes/retail_demo/landing/kaggle_raw/fashion_boutique_dataset.csv"

# Read CSV as-is
df = (spark.read.format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(src))

# Lineage / partitions
now = F.current_timestamp()
df = (df.withColumn("ingest_ts", now)
        .withColumn("batch_id", F.date_format(now, "yyyyMMddHHmmss"))
        .withColumn("source_file", F.lit(src))
        .withColumn("ingest_date", F.to_date(now)))

# ---------------- bronze.products ----------------
bp = (df.select("product_id","category","brand","season","size","color","original_price",
                "ingest_ts","batch_id","source_file","ingest_date")
        .dropDuplicates(["product_id"]))

(bp.write
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("ingest_date")
   .format("delta")
   .saveAsTable("retail_demo.bronze.products"))

# ---------------- bronze.sales ----------------
# synthesize order_id from product_id + purchase timestamp
bs = (df.select("product_id","purchase_date","current_price","markdown_percentage",
                "customer_rating","is_returned","return_reason","stock_quantity",
                "ingest_ts","batch_id","source_file","ingest_date")
        .withColumn("purchase_ts", F.to_timestamp("purchase_date"))
        .withColumn("order_id",
            F.sha2(F.concat_ws("|",
                F.col("product_id").cast("string"),
                F.date_format(F.col("purchase_ts"), "yyyy-MM-dd HH:mm:ss")
            ), 256))
     )

(bs.write
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("ingest_date")
   .format("delta")
   .saveAsTable("retail_demo.bronze.sales"))

# ---------------- bronze.return_reasons ----------------
br = (df.select("return_reason","ingest_ts","batch_id","source_file","ingest_date")
        .where(F.col("return_reason").isNotNull())
        .dropDuplicates(["return_reason"]))

(br.write
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("_ingest_date" if False else "ingest_date")
   .format("delta")
   .saveAsTable("retail_demo.bronze.return_reasons"))

print("Bronze tables written: retail_demo.bronze.{products, sales, return_reasons}")

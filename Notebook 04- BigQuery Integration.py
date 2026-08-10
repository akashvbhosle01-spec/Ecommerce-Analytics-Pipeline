# Databricks notebook source
RAW_PATH = "/Volumes/workspace/default/workspace/"
BRONZE_PATH = "/Volumes/workspace/default/workspace/bronze/"
SILVER_PATH = "/Volumes/workspace/default/workspace/silver/"
GOLD_PATH = "/Volumes/workspace/default/workspace/gold/"

GCP_PROJECT = "gifted-decker-503209-k7"
BQ_DATASET = "ecommerce"
TEMP_GCS_BUCKET = "ecommerce-databricks-temp"

GCP_SECRET_SCOPE = "gcp-secrets"
GCP_SECRET_KEY = "gcp-sa-key"

# COMMAND ----------

gcp_sa_key = dbutils.secrets.get(scope=GCP_SECRET_SCOPE, key=GCP_SECRET_KEY)
with open ("/Volumes/workspace/default/workspace/gifted-decker-503209-k7-35a4e887841c.json", "w") as f:
    f.write(gcp_sa_key)
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enabled", "True")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Volumes/workspace/default/workspace/gifted-decker-503209-k7-35a4e887841c.json")


# COMMAND ----------

# अपनी Notebook में मौजूद सभी Variables की List देखें
%who_ls DataFrame

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/workspace/gold/

# COMMAND ----------

# 1️⃣ Silver Table को Catalog से Read करें (यह पक्का मौजूद है)
silver = spark.sql("SELECT * FROM workspace.default.transactions_enriched")

# 2️⃣ Daily Revenue Gold Table बनाएं (अगर नहीं है तो)
from pyspark.sql.functions import sum, round

gold_daily_revenue = (silver.groupBy("transaction_date")
                      .agg(round(sum("final_amount"), 2).alias("revenue"))
                      .orderBy("transaction_date"))

# 3️⃣ Gold Table को Delta में Save करें (PATH_NOT_FOUND ठीक हो जाएगा)
gold_daily_revenue.write.mode("overwrite").format("delta").save(GOLD_PATH + "daily_revenue")
print("✅ Gold Table (daily_revenue) Successfully Saved!")

# 4️⃣ अब CSV Export करें
gold_daily_revenue.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/daily_revenue")
print("✅ CSV Export Complete! अब Databricks UI से CSV Download करें।")

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, col, round, desc, max as _max, count as _count, sum as _sum, datediff, current_date, when

# Silver Table Read करें और Temp View बनाएं
silver = spark.sql("SELECT * FROM workspace.default.transactions_enriched")
silver.createOrReplaceTempView("silver_trn")

print("⏳ सारी Gold Tables बन रही हैं और CSV Export हो रही हैं...")

# ---------- 1. DAILY REVENUE ----------
gold_daily = silver.groupBy("transaction_date").agg(round(sum("final_amount"), 2).alias("revenue")).orderBy("transaction_date")
gold_daily.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "daily_revenue")
gold_daily.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/daily_revenue")

# ---------- 2. CATEGORY SALES ----------
gold_cat = silver.groupBy("category").agg(round(sum("final_amount"), 2).alias("total_revenue"), count("transaction_id").alias("order_count")).orderBy(desc("total_revenue"))
gold_cat.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "category_sales")
gold_cat.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/category_sales")

# ---------- 3. REGION PERFORMANCE ----------
gold_reg = silver.groupBy("region").agg(round(sum("final_amount"), 2).alias("region_revenue"), round(avg("final_amount"), 2).alias("avg_order_value")).orderBy(desc("region_revenue"))
gold_reg.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "region_performance")
gold_reg.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/region_performance")

# ---------- 4. TOP PRODUCTS ----------
gold_prod = silver.groupBy("product_name", "category").agg(round(sum("final_amount"), 2).alias("total_sales"), count("transaction_id").alias("units_sold"), round(avg("rating"), 2).alias("avg_rating")).orderBy(desc("total_sales"))
gold_prod.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "product_performance")
gold_prod.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/product_performance")

# ---------- 5. STORE + CATEGORY ----------
gold_store_cat = spark.sql("""
    SELECT transaction_date, store_name, location AS store_location, category,
           ROUND(SUM(total_amount), 2) AS gross_sales,
           ROUND(SUM(final_amount), 2) AS net_sales,
           COUNT(DISTINCT customer_id) AS unique_customers,
           COUNT(*) AS txn_count
    FROM silver_trn
    GROUP BY transaction_date, store_name, store_location, category
""")
gold_store_cat.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "daily_store_cat")
gold_store_cat.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/daily_store_cat")

# ---------- 6. TOP CUSTOMERS ----------
gold_cust = spark.sql("""
    SELECT customer_id, customer_name, region, store_name,
           ROUND(SUM(final_amount), 2) AS total_spent, COUNT(*) AS txn_count
    FROM silver_trn
    GROUP BY customer_id, customer_name, region, store_name
    ORDER BY total_spent DESC
""")
gold_cust.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "top_customers")
gold_cust.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/top_customers")

# ---------- 7. PRODUCT SENTIMENT ----------
gold_sent = spark.sql("""
    SELECT product_id, product_name, category,
           ROUND(AVG(rating), 2) AS avg_rating, COUNT(rating) AS rating_count
    FROM silver_trn
    WHERE rating IS NOT NULL
    GROUP BY product_id, product_name, category
    ORDER BY avg_rating DESC
""")
gold_sent.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "product_sentiment")
gold_sent.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/product_sentiment")

# ---------- 8. PROMO PERFORMANCE ----------
gold_promo = spark.sql("""
    SELECT promotion_id, COUNT(*) AS promo_txns,
           ROUND(SUM(final_amount), 2) AS promo_sales,
           ROUND(AVG(final_amount), 2) AS avg_with_promo
    FROM silver_trn
    GROUP BY promotion_id
    ORDER BY promo_sales DESC
""")
gold_promo.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "promo_performance")
gold_promo.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/promo_performance")

# ---------- 9. RFM ANALYSIS ----------
rfm = (silver.groupBy("customer_id", "customer_name")
    .agg(_max("transaction_date").alias("last_txn"), _count("transaction_id").alias("frequency"), _sum("final_amount").alias("monetary"))
    .withColumn("recency_days", datediff(current_date(), col("last_txn")))
)
rfm_bucketed = (rfm
    .withColumn("recency_bucket", when(col("recency_days") <= 30, "0-30").when(col("recency_days") <= 90, "31-90").otherwise("90+"))
    .withColumn("frequency_bucket", when(col("frequency") >= 10, "high").when(col("frequency") >= 3, "medium").otherwise("low"))
    .withColumn("monetary_bucket", when(col("monetary") >= 1000, "high").when(col("monetary") >= 50, "medium").otherwise("low"))
)
rfm_bucketed.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(GOLD_PATH + "rfm_analysis")
rfm_bucketed.write.mode("overwrite").option("header", True).csv(GOLD_PATH + "export/rfm_analysis")

print("\n🎉 सभी 9 Gold Tables Delta में Save और CSV में Export हो गईं!")
print("📁 अब Databricks UI में 'gold/export' फोल्डर से CSV Download करें!")

# COMMAND ----------


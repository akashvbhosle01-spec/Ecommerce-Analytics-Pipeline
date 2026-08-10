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

from pyspark.sql.functions import sum as _sum, countDistinct, avg, desc
silver = spark.read.format("delta").load(SILVER_PATH + "transactions_enriched")
silver.createOrReplaceTempView("silver_trn")

# COMMAND ----------

gold_daily_store_cat = spark.sql("""
    SELECT 
        transaction_date, 
        store_name, 
        location AS store_location, 
        category,
        SUM(total_amount) AS gross_sales,
        SUM(final_amount) AS net_sales,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(*) AS txn_count
    FROM silver_trn
    GROUP BY transaction_date, store_name, store_location, category
""")

display(gold_daily_store_cat)


# COMMAND ----------

# 🎯 Gold Table को Delta Format में Save करें (कोई Error नहीं आएगा)
gold_daily_store_cat.write.mode("overwrite").format("delta").save(GOLD_PATH + "daily_store_cat")

print("✅ Gold Table (daily_store_cat) Successfully Saved to Delta!")

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/workspace/gold/daily_store_cat

# COMMAND ----------

gold_top_customers = spark.sql("""
    SELECT 
        customer_id, 
        customer_name,   -- 🔥 FIX: 'name' की जगह 'customer_name' (जो वास्तविक Column है)
        region, 
        store_name, 
        ROUND(SUM(final_amount), 2) AS total_spent, 
        COUNT(*) AS txn_count 
    FROM silver_trn
    GROUP BY customer_id, customer_name, region, store_name  -- 🔥 FIX: यहाँ भी 'customer_name'
    ORDER BY total_spent DESC
""")

display(gold_top_customers)

# Save to Gold Path
gold_top_customers.write.mode("overwrite").format("delta").save(GOLD_PATH + "top_customers")

print("✅ Top Customers Gold Table Saved Successfully!")

# COMMAND ----------

# Silver View की सारी Columns Check करें
print(spark.sql("DESCRIBE silver_trn").show())

# COMMAND ----------

gold_promo_all = spark.sql("""
    SELECT 
        promotion_id, 
        COUNT(*) AS promo_trns,
        ROUND(SUM(final_amount), 2) AS promo_sales,
        ROUND(AVG(final_amount), 2) AS avg_promo_sales
    FROM silver_trn
    GROUP BY promotion_id
""")

display(gold_promo_all)

# COMMAND ----------

gold_sentiment = spark.sql("""
                           select product_id, product_name, category, avg(rating) as avg_rating,
                           count(rating) as rating_count
                           from silver_trn
                           where rating is not null
                           group by product_id, product_name, category
                           """)
gold_sentiment.display()

# COMMAND ----------

gold_sentiment.write.mode("overwrite") \
    .format("delta") \
    .save(GOLD_PATH + "product_sentiment")

print(f"✅ Product Sentiment Gold Table Successfully Saved at: {GOLD_PATH}product_sentiment")

# COMMAND ----------

from pyspark.sql.functions import max as _max, count as _count, sum as _sum, datediff, current_date, col, when

# 1️⃣ RFM Aggregation: हर Customer के लिए Last Transaction, Frequency, और Monetary Value निकालो
# 🔥 FIX: 'name' की जगह 'customer_name' (क्योंकि Silver Table में यही Column है)
rfm = (silver.groupBy("customer_id", "customer_name")
    .agg(
        _max("transaction_date").alias("last_txn"),      # Recency के लिए आखिरी तारीख
        _count("transaction_id").alias("frequency"),     # Frequency (कितनी बार खरीदा)
        _sum("final_amount").alias("monetary")           # Monetary (कुल पैसा खर्च)
    )
    .withColumn("recency_days", datediff(current_date(), col("last_txn")))  # आज से कितने दिन पहले?
)

# 2️⃣ Bucketing: Recency, Frequency, Monetary को 3-3 Categories (बकेट) में बाँटो
rfm_bucketed = (rfm
    .withColumn("recency_bucket", 
        when(col("recency_days") <= 30, "0-30")
        .when(col("recency_days") <= 90, "31-90")
        .otherwise("90+")
    )
    .withColumn("frequency_bucket", 
        when(col("frequency") >= 10, "high")
        .when(col("frequency") >= 3, "medium")
        .otherwise("low")
    )
    .withColumn("monetary_bucket", 
        when(col("monetary") >= 1000, "high")
        .when(col("monetary") >= 50, "medium")
        .otherwise("low")
    )
)

# 3️⃣ Result दिखाओ
display(rfm_bucketed)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, col

w = Window.partitionBy("customer_id").orderBy("transaction_date")

txn_with_prev = silver.withColumn("prev_txn", lag("transaction_date").over(w)) \
                      .withColumn("prev_store", lag("store_id").over(w))

txn_with_prev = txn_with_prev.withColumn(
    "time_diff_mins", 
    (unix_timestamp("transaction_date") - unix_timestamp("prev_txn")) / 60
)
suspects = txn_with_prev.filter(
    (col("time_diff_mins") < 30) &          
    (col("total_amount") > 1000) &          
    (col("store_id") != col("prev_store"))  
)
suspects.write.mode("overwrite") \
    .format("delta") \
    .save(GOLD_PATH + "suspects")
display(suspects)

# COMMAND ----------

display(txn_with_prev)

# COMMAND ----------


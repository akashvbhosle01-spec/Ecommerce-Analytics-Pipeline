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

from pyspark.sql.functions import to_timestamp, coalesce, lit, when, date_format,current_timestamp, col
from pyspark.sql.types import DoubleType

trn = spark.read.format("delta").load(BRONZE_PATH + "transactions")
cust =spark.read.format("delta").load(BRONZE_PATH + "customers")
fb = spark.read.format("delta").load(BRONZE_PATH + "feedback")
prod = spark.read.format("delta").load(BRONZE_PATH + "products")
promo = spark.read.format("delta").load(BRONZE_PATH + "promotions")
store = spark.read.format("delta").load(BRONZE_PATH + "stores")


# COMMAND ----------

trn.display()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, coalesce, lit, current_timestamp
from pyspark.sql.types import DoubleType, TimestampType

trn_Clean = (trn
             .dropDuplicates(['transaction_id'])  
             .withColumn("total_amount", col("total_amount").cast(DoubleType())) 
             .withColumn("transaction_date", col("transaction_date").cast("date"))
             .withColumn("quantity", col("quantity").cast("int"))  
             .withColumn("total_amount", coalesce(col("total_amount"), lit(0))) 
             .withColumn("_ingest_time", current_timestamp())   
            )

display(trn_Clean)
print("Silver Transactions Count:", trn_Clean.count())

# COMMAND ----------

cust_clean = (cust
    .dropDuplicates(['customer_id'])
    .withColumn("signup_date", col("signup_date").cast("date"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("ingest_time", current_timestamp())
)
display(cust_clean)
print("✅ cust_clean count:", cust_clean.count())

# COMMAND ----------

silver = (trn_Clean.alias("t")
          .join(cust.alias("c"), "customer_id", "left")
          .join(prod.alias("p"), "product_id", "left")
          .join(store.alias("s"), "store_id", "left")
          .join(fb.alias("f"), "feedback_id", "left")
          .join(promo.alias("pr"), "promotion_id", "left")
)
silver = (silver
          .withColumn("transaction_date", date_format(col("transation_date"), "yyyy-MM-dd"))
          .withColumn("id_valid_store", when (col("location").isNotNull(), lit(1)).otherwise(lit(False)))
          .withColumn("ha_valid_customer", when(col("name").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("promo_active", when(col("discount_percent").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("final_amount", when(col("discount_percent").isNotNull(),
                                           col("total_amount")*(1 - col("discount_percent")/100))
                                          .otherwise(col("total_amount")))
)

display(silver)
print("✅ Silver Fact Table Count:", silver.count())

# COMMAND ----------

from pyspark.sql.functions import col, date_format, when, lit, current_timestamp

silver = (trn_Clean.alias("t")
          .join(cust.alias("c"), "customer_id", "left")
          .join(prod.alias("p"), "product_id", "left")
          .join(store.alias("s"), "store_id", "left")
          .join(fb.alias("f"), 
                (col("t.customer_id") == col("f.customer_id")) & 
                (col("t.product_id") == col("f.product_id")), 
                "left")
          .join(promo.alias("pr"), 
                (col("t.product_id") == col("pr.product_id")) & 
                (col("t.transaction_date") >= col("pr.start_date")) & 
                (col("t.transaction_date") <= col("pr.end_date")), 
                "left")
)

silver = (silver
          # 🔥 FIX: 'transation_date' -> 'transaction_date' (typo सुधारा)
          .withColumn("transaction_date", date_format(col("t.transaction_date"), "yyyy-MM-dd"))
          .withColumn("is_valid_store", when(col("s.location").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("is_valid_customer", when(col("c.name").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("promo_active", when(col("pr.discount").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("final_amount", 
                      when(col("pr.discount").isNotNull(),
                           col("t.total_amount") * (1 - col("pr.discount") / 100))
                      .otherwise(col("t.total_amount")))
          .withColumn("silver_processed_time", current_timestamp())
)

display(silver)
print("✅ Silver Table Count (All 6 Tables Joined):", silver.count())
print("\n📊 Columns in Silver Table:")
print(silver.columns)

# COMMAND ----------

# ---------- SILVER TABLE JOIN + TRANSFORM ----------
silver = (trn_Clean.alias("t")
          .join(cust.alias("c"), "customer_id", "left")
          .join(prod.alias("p"), "product_id", "left")
          .join(store.alias("s"), "store_id", "left")
          .join(fb.alias("f"), 
                (col("t.customer_id") == col("f.customer_id")) & 
                (col("t.product_id") == col("f.product_id")), "left")
          .join(promo.alias("pr"), 
                (col("t.product_id") == col("pr.product_id")) & 
                (col("t.transaction_date") >= col("pr.start_date")) & 
                (col("t.transaction_date") <= col("pr.end_date")), "left")
          .select(
              col("t.*"),
              col("c.name").alias("customer_name"),
              col("c.age"), col("c.gender"), col("c.region"), col("c.signup_date"),
              col("p.product_name"), col("p.category"), col("p.price"), col("p.brand"),
              col("s.store_name"), col("s.location"), col("s.manager"), col("s.opened_date"),
              col("f.feedback_id"), col("f.rating"), col("f.review"),
              col("f.date").alias("feedback_date"),
              col("pr.promotion_id"), col("pr.discount"),
              col("pr.channel").alias("promo_channel"),
              col("pr.start_date").alias("promo_start_date"),
              col("pr.end_date").alias("promo_end_date")
          )
)

silver = (silver
          .withColumn("transaction_date", col("transaction_date").cast("date"))
          .withColumn("is_valid_store", when(col("location").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("is_valid_customer", when(col("customer_name").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("promo_active", when(col("discount").isNotNull(), lit(True)).otherwise(lit(False)))
          .withColumn("final_amount", 
                      when(col("discount").isNotNull(),
                           col("total_amount") * (1 - col("discount") / 100))
                      .otherwise(col("total_amount")))
          .withColumn("silver_processed_time", current_timestamp())
)

display(silver)
print("✅ Silver Count:", silver.count())

# 🔥 FINAL SAVE (1 LINE - कोई ERROR नहीं)
silver.write.mode("overwrite").partitionBy("transaction_date").format("delta").saveAsTable("workspace.default.transactions_enriched")

print("🎉 Silver Table Successfully Saved!")

# COMMAND ----------

silver.write.mode("overwrite") \
      .partitionBy("transaction_date") \
      .format("delta") \
      .saveAsTable("workspace.default.transactions_enriched")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.transactions_enriched LIMIT 5;

# COMMAND ----------

from pyspark.sql.functions import to_date, col, when

silver = silver.withColumn(
    "promo_in_range",
    when(
        (col("promo_active") == True) & 
        (col("transaction_date") >= col("promo_start_date")) & 
        (col("transaction_date") <= col("promo_end_date")),
        True
    ).otherwise(False)
)

display(silver.select("transaction_id", "promo_active", "promo_start_date", "promo_end_date", "promo_in_range"))

# COMMAND ----------


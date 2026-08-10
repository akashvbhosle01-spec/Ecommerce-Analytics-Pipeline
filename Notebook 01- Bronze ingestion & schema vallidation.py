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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit

trn_schema =  StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),  
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", DoubleType(), True)
])
cust_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", DateType(), True) 
])
prod_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("brand", StringType(), True)
])
store_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("store_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("manager", StringType(), True),
    StructField("opened_date", DateType(), True)  
])
promo_schema = StructType([
    StructField("promotion_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("discount", DoubleType(), True),
    StructField("start_date", DateType(), True),  
    StructField("end_date", DateType(), True),    
    StructField("channel", StringType(), True)
])
fb_schema = StructType([
    StructField("feedback_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review", StringType(), True),
    StructField("date", DateType(), True) 
])

trn_df = spark.read.option("header", True)\
    .option("dateFormat", "dd-MM-yyyy")\
    .schema(trn_schema).csv(f"{RAW_PATH}transactions.csv")

cust_df = spark.read.option("header", True)\
    .option("dateFormat", "dd-MM-yyyy")\
    .schema(cust_schema).csv(f"{RAW_PATH}customers.csv")

prod_df = spark.read.option("header", True)\
    .schema(prod_schema).csv(f"{RAW_PATH}products.csv") 

store_df = spark.read.option("header", True)\
    .option("dateFormat", "dd-MM-yyyy")\
    .schema(store_schema).csv(f"{RAW_PATH}stores.csv")

promo_df = spark.read.option("header", True)\
    .option("dateFormat", "dd-MM-yyyy")\
    .schema(promo_schema).csv(f"{RAW_PATH}promotions.csv")

fb_df = spark.read.option("header", True)\
    .option("dateFormat", "dd-MM-yyyy")\
    .schema(fb_schema).csv(f"{RAW_PATH}feedback.csv")   ा

print("✅ Transactions:", trn_df.count())
print("✅ Customers:", cust_df.count())
print("✅ Products:", prod_df.count())
print("✅ Stores:", store_df.count())
print("✅ Promotions:", promo_df.count())
print("✅ Feedbacks:", fb_df.count())

print("\n📊 Schema Check (Transactions):")
trn_df.printSchema()

print("\n🔍 Sample Data (Transactions) 
trn_df.show(5, truncate=False)

print("\n✅ Null Check (Transaction Date):")
trn_df.filter("transaction_date IS NULL").show()


trn_df.selectExpr("count(distinct transaction_id) as unique_trns").show()
trn_df.filter("transaction_date IS NULL or total_amount IS NULL").show()


(trn_df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").save(BRONZE_PATH + "transactions"))
(cust_df.write.mode("overwrite").format("delta").save(BRONZE_PATH + "customers"))
(prod_df.write.mode("overwrite").format("delta").save(BRONZE_PATH + "products"))
(store_df.write.mode("overwrite").format("delta").save(BRONZE_PATH + "stores"))
(promo_df.write.mode("overwrite").format("delta").save(BRONZE_PATH + "promotions"))
(fb_df.write.mode("overwrite").format("delta").save(BRONZE_PATH + "feedback"))



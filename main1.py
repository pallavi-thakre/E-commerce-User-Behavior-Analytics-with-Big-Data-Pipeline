from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, to_date, countDistinct

# Step 1: Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Event Analysis") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Step 2: Load JSONL files
df = spark.read.json("/home/pallavi/Desktop/bigdat_project/aggregated_output/*.jsonl")
df = df.withColumn("timestamp", to_timestamp("timestamp")).dropna()

print("✅ Records loaded:", df.count())
print("✅ Columns:", df.columns)
df.show(3, truncate=False)

# Step 3: Event type funnel analysis
print("🧭 Conversion Funnel (Unique Users per Event Type):")
df.groupBy("event_type").agg(countDistinct("user_id").alias("unique_users")).show()

# Step 4: Top product per city by view count
print("🏙️ Top viewed product per location:")
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

view_df = df.filter(col("event_type") == "view")
windowSpec = Window.partitionBy("location").orderBy(col("count").desc())

top_viewed = view_df.groupBy("location", "product_id") \
    .count() \
    .withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") == 1)

top_viewed.select("location", "product_id", "count").show()

# Step 5: Read and analyze daily_revenue.csv (if applicable)
rev_path = "/home/pallavi/Desktop/bigdat_project/aggregated_output/daily_revenue.csv"
revenue_df = spark.read.option("header", True).csv(rev_path)
print("💰 Daily Revenue:")
revenue_df.show()

# Step 6: Save funnel and product data to CSV (optional)
df.groupBy("event_type").agg(countDistinct("user_id").alias("unique_users")) \
    .coalesce(1).write.mode("overwrite").csv("/home/pallavi/Desktop/bigdat_project/output/funnel_data", header=True)

top_viewed.coalesce(1).write.mode("overwrite").csv("/home/pallavi/Desktop/bigdat_project/output/top_products", header=True)

# Step 7: Done
spark.stop()









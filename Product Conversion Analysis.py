import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# ✅ Step 1: Start SparkSession
spark = SparkSession.builder \
    .appName("Product Conversion Analysis") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Step 2: Load JSONL data
df = spark.read.json("/home/pallavi/Desktop/bigdat_project/aggregated_output/*.jsonl")

# check schema
df.printSchema()
df.show(5)


# Step 3: Get view and purchase counts per product
viewed = df.filter(col("event_type") == "view") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "view_count")

purchased = df.filter(col("event_type") == "purchase") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "purchase_count")

# Step 4: Join both
conversion = viewed.join(purchased, on="product_id", how="left").fillna(0)

# Step 5: Calculate conversion rate
conversion = conversion.withColumn(
    "conversion_rate", col("purchase_count") / col("view_count")
)

# Step 6: Show top and bottom products by conversion
print("🔥 Top 10 Products by Conversion Rate:")
conversion.orderBy(col("conversion_rate").desc()).show(10, truncate=False)

print("❄️ Bottom 10 Products by Conversion Rate (excluding 0 views):")
conversion.filter(col("view_count") > 0).orderBy(col("conversion_rate").asc()).show(10, truncate=False)

# Top 10 products by conversion rate
top_products = conversion.orderBy(col("conversion_rate").desc()).limit(10)
top_products_pd = top_products.toPandas()

sns.barplot(data=top_products_pd, x="product_id", y="conversion_rate")
plt.title("Top 10 Products by Conversion Rate")
plt.ylabel("Conversion Rate")
plt.xlabel("Product ID")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()





# Step 1: Views per location
views_by_city = df.filter(col("event_type") == "view") \
    .groupBy("location") \
    .agg(count("*").alias("view_count"))

# Step 2: Purchases per location
purchases_by_city = df.filter(col("event_type") == "purchase") \
    .groupBy("location") \
    .agg(count("*").alias("purchase_count"))

# Step 3: Join and calculate conversion rate
city_conversion = views_by_city.join(purchases_by_city, on="location", how="left").fillna(0)

city_conversion = city_conversion.withColumn(
    "conversion_rate", col("purchase_count") / col("view_count")
)

# Step 4: Show results
print("📍 Conversion Rate by City:")
city_conversion.orderBy(col("conversion_rate").desc()).show(truncate=False)

# Convert to pandas
city_conversion_pd = city_conversion.toPandas()

# Plot

sns.barplot(data=city_conversion_pd, x="location", y="conversion_rate")
plt.title("Conversion Rate by City")
plt.ylabel("Conversion Rate")
plt.xlabel("City")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()



# Step 1: Views per device
views_by_device = df.filter(col("event_type") == "view") \
    .groupBy("device") \
    .agg(count("*").alias("view_count"))

# Step 2: Purchases per device
purchases_by_device = df.filter(col("event_type") == "purchase") \
    .groupBy("device") \
    .agg(count("*").alias("purchase_count"))

# Step 3: Join and calculate conversion rate
device_conversion = views_by_device.join(purchases_by_device, on="device", how="left").fillna(0)

device_conversion = device_conversion.withColumn(
    "conversion_rate", col("purchase_count") / col("view_count")
)

# Step 4: Show results
print("📱💻 Conversion Rate by Device:")
device_conversion.orderBy(col("conversion_rate").desc()).show(truncate=False)


device_conversion_pd = device_conversion.toPandas()


sns.barplot(data=device_conversion_pd, x="device", y="conversion_rate")
plt.title("Conversion Rate by Device")
plt.ylabel("Conversion Rate")
plt.xlabel("Device Type")
plt.show()



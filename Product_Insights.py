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

from pyspark.sql.functions import col

# View count per product
views = df.filter(col("event_type") == "view") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "view_count")

# Purchase count per product
purchases = df.filter(col("event_type") == "purchase") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "purchase_count")

# Join and find products with views but no purchases
view_no_purchase = views.join(purchases, on="product_id", how="left").fillna(0) \
    .filter(col("purchase_count") == 0) \
    .orderBy(col("view_count").desc())

print("=" * 60)
print("👀 Top 10 Products Most Viewed but Never Purchased")
print("=" * 60)
view_no_purchase.show(10, truncate=False)


# Add-to-cart count per product
add_cart = df.filter(col("event_type") == "add_to_cart") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "cart_count")

# Join with purchase count
cart_vs_purchase = add_cart.join(purchases, on="product_id", how="left").fillna(0)

# Products with high cart count but low purchases
low_conversion_cart = cart_vs_purchase \
    .filter(col("purchase_count") < 5) \
    .orderBy(col("cart_count").desc())

print("=" * 60)
print("🛒 Most Added to Cart But Purchased Less Than 5 Times")
print("=" * 60)
low_conversion_cart.show(10, truncate=False)

from pyspark.sql.functions import col

# ✅ Add to cart count
add_cart = df.filter(col("event_type") == "add_to_cart") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "cart_count")

# ✅ Purchase count
purchases = df.filter(col("event_type") == "purchase") \
    .groupBy("product_id") \
    .count() \
    .withColumnRenamed("count", "purchase_count")

# ✅ Join and calculate conversion rate
conversion = add_cart.join(purchases, on="product_id", how="left").fillna(0)
conversion = conversion.withColumn("conversion_rate", col("purchase_count") / col("cart_count"))

# ✅ Show best converting products
print("=" * 60)
print("🔥 Top 10 Products by Add-to-Cart → Purchase Conversion Rate")
print("=" * 60)
conversion.orderBy(col("conversion_rate").desc()).show(10, truncate=False)




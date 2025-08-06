import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# ✅ Step 1: Start SparkSession
spark = SparkSession.builder \
    .appName("CityWisePurchaseCounts") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Step 2: Load JSONL data
df = spark.read.json("/home/pallavi/Desktop/bigdat_project/aggregated_output/*.jsonl")

# ✅ Step 3: Filter only purchase events
purchase_df = df.filter(col("event_type") == "purchase")

# ✅ Step 4: Group by location and count purchases
city_purchase_count = purchase_df.groupBy("location") \
    .agg(count("*").alias("total_purchases")) \
    .orderBy(col("total_purchases").desc())

# ✅ Step 5: Show output table
print("=" * 60)
print("🏙️ City-wise Total Purchase Counts")
print("=" * 60)
city_purchase_count.show(truncate=False)

# ✅ Step 6: Convert to Pandas (do this before stopping Spark)
city_purchase_pd = city_purchase_count.toPandas()

# ✅ Step 7: Plotting
plt.figure(figsize=(10, 6))
sns.barplot(data=city_purchase_pd, x="location", y="total_purchases", palette="viridis")
plt.xlabel("City")
plt.ylabel("Total Purchases")
plt.title("🏙️ City-wise Purchase Counts")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ✅ Step 8: Stop Spark session
spark.stop()



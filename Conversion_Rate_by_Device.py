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
from pyspark.sql.functions import countDistinct

# Count users by event type and device
device_conversion = df.groupBy("device", "event_type") \
    .agg(countDistinct("user_id").alias("unique_users"))

# Pivot for better structure: one row per device
device_funnel = device_conversion.groupBy("device") \
    .pivot("event_type", ["view", "add_to_cart", "purchase"]) \
    .sum("unique_users") \
    .fillna(0)

# Add conversion rate: purchase / view
device_funnel = device_funnel.withColumn(
    "conversion_rate", col("purchase") / col("view")
)

# Plot using Seaborn
device_pd = device_funnel.select("device", "conversion_rate").toPandas()

sns.barplot(data=device_pd, x="device", y="conversion_rate", palette="pastel")
plt.title("📱 Conversion Rate by Device")
plt.xlabel("Device")
plt.ylabel("Conversion Rate")
plt.tight_layout()
plt.show()

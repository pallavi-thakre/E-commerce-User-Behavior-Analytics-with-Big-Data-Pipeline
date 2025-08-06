import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Start Spark session
spark = SparkSession.builder \
    .appName("UserFunnelVisualization") \
    .master("local[*]") \
    .getOrCreate()

# Load the data
df = spark.read.json("/home/pallavi/Desktop/bigdat_project/aggregated_output/*.jsonl")

# Unique user count per event_type
funnel_counts = df.groupBy("event_type") \
    .agg(countDistinct("user_id").alias("unique_users"))

# Convert to pandas
funnel_pd = funnel_counts.toPandas()

# Set funnel order
funnel_pd["event_type"] = pd.Categorical(funnel_pd["event_type"], ["view", "add_to_cart", "purchase"])
funnel_pd = funnel_pd.sort_values("event_type")

# Plot
sns.barplot(data=funnel_pd, x="event_type", y="unique_users", palette="Blues_d")
plt.title("🔄 User Funnel Stages")
plt.xlabel("Funnel Stage")
plt.ylabel("Unique Users")
plt.tight_layout()
plt.show()

# Stop Spark
spark.stop()


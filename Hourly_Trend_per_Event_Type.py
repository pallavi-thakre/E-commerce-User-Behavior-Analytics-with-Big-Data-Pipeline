import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct,count



# Start Spark session
spark = SparkSession.builder \
    .appName("UserFunnelVisualization") \
    .master("local[*]") \
    .getOrCreate()

# Load the data
df = spark.read.json("/home/pallavi/Desktop/bigdat_project/aggregated_output/*.jsonl")

from pyspark.sql.functions import hour

# Extract hour from timestamp
df_with_hour = df.withColumn("hour", hour("timestamp"))

# Count events per hour and type
hourly_trend = df_with_hour.groupBy("hour", "event_type") \
    .agg(count("*").alias("count")) \
    .orderBy("hour")

# Convert to Pandas
hourly_pd = hourly_trend.toPandas()

# Plot using seaborn
plt.figure(figsize=(12, 6))
sns.lineplot(data=hourly_pd, x="hour", y="count", hue="event_type", marker="o")
plt.title("🕒 Hourly Trend by Event Type")
plt.xlabel("Hour of Day")
plt.ylabel("Event Count")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.show()

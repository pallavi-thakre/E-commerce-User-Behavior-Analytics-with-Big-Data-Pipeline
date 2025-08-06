from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# ✅ Step 1: Start SparkSession
spark = SparkSession.builder \
    .appName("Daily Event Trend Analysis") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Step 2: Load JSONL data
df = spark.read.json("/home/pallavi/Desktop/bigdat_project/aggregated_output/*.jsonl")

# ✅ Step 3: View schema and preview
df.printSchema()
df.show(5)

# ✅ Step 4: Extract date from timestamp
df = df.withColumn("date", to_date(col("timestamp")))
df.show(5)

# ✅ Step 5: Aggregate events per date and event_type
daily_event_trend = df.groupBy("date", "event_type") \
    .agg(count("*").alias("event_count")) \
    .orderBy("date")

# ✅ Step 6: Show result
print("=" * 60)
print("📊 Daily Event Counts by Event Type")
print("=" * 60)
daily_event_trend.show(50, truncate=False)

# ✅ Step 7: Convert to Pandas for plotting
daily_event_trend_pd = daily_event_trend.toPandas()

# ✅ Step 8: Plot
plt.figure(figsize=(12, 6))
sns.lineplot(data=daily_event_trend_pd, x="date", y="event_count", hue="event_type", marker="o")
plt.title("📈 Daily Event Trend")
plt.xlabel("Date")
plt.ylabel("Number of Events")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ✅ Step 9: Stop Spark Session
spark.stop()







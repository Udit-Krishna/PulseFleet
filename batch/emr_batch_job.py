# batch/emr_batch_job.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count, hour

spark = SparkSession.builder.appName("PulseFleetBatch").getOrCreate()

df = spark.read.json("s3://pulsefleet-project-bucket-uditks/raw-events/")

completed = df.filter(col("event_type") == "trip_completed") \
    .withColumn("trip_date", to_date("timestamp")) \
    .withColumn("trip_hour", hour("timestamp"))

daily_city_revenue = completed.groupBy("trip_date", "city") \
    .agg(_sum("fare").alias("total_revenue"), count("event_id").alias("total_trips"))

hourly_demand = completed.groupBy("trip_date", "trip_hour", "city") \
    .agg(count("event_id").alias("trip_count"))

daily_city_revenue.write.mode("overwrite").parquet("s3://pulsefleet-project-bucket-uditks/curated/daily_revenue/")
hourly_demand.write.mode("overwrite").parquet("s3://pulsefleet-project-bucket-uditks/curated/hourly_demand/")
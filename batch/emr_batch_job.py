import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count, hour, lit


def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: emr_batch_job.py <run_date YYYY-MM-DD>")

    run_date = sys.argv[1]
    year, month, day = run_date.split("-")

    spark = SparkSession.builder.appName("PulseFleetBatch").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    input_path = (
        f"s3://pulsefleet-project-bucket-uditks/raw-events/"
        f"year={year}/month={month}/day={day}/"
    )

    df = spark.read.json(input_path)

    if df.rdd.isEmpty():
        print(f"No data for {run_date}, exiting.")
        spark.stop()
        return

    completed = df.filter(col("event_type") == "trip_completed") \
        .withColumn("trip_date", to_date(lit(run_date))) \
        .withColumn("trip_hour", hour("timestamp"))

    daily_city_revenue = completed.groupBy("trip_date", "city") \
        .agg(
            _sum("fare").alias("total_revenue"),
            count("event_id").alias("total_trips"),
        )

    hourly_demand = completed.groupBy("trip_date", "trip_hour", "city") \
        .agg(count("event_id").alias("trip_count"))

    daily_city_revenue.write \
        .mode("overwrite") \
        .partitionBy("trip_date") \
        .parquet("s3://pulsefleet-project-bucket-uditks/curated/daily_revenue/")

    hourly_demand.write \
        .mode("overwrite") \
        .partitionBy("trip_date") \
        .parquet("s3://pulsefleet-project-bucket-uditks/curated/hourly_demand/")

    print(f"Done. daily_revenue={daily_city_revenue.count()}, hourly_demand={hourly_demand.count()}")
    spark.stop()


if __name__ == "__main__":
    main()

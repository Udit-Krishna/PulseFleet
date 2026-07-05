from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("PulseFleetStreaming")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
            "org.apache.hadoop:hadoop-aws:3.4.2"
        ])
    )
    .getOrCreate()
)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
)
hadoop_conf.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
hadoop_conf.set("fs.s3a.endpoint.region", "ap-south-1")


schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("city", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("fare", DoubleType()) \
    .add("distance_km", DoubleType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ride-events") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Rolling 5-min window: active trips + revenue per city
metrics = parsed.filter(col("event_type") == "trip_completed") \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("city")) \
    .agg(
        count("event_id").alias("completed_trips"),
        avg("fare").alias("avg_fare")
    )

query = metrics.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "s3a://pulsefleet-project-bucket-uditks/streaming-output/") \
    .option("checkpointLocation", "s3a://pulsefleet-project-bucket-uditks/checkpoints/") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()
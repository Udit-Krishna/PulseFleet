from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, date_format
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
spark.sparkContext.setLogLevel("ERROR")

# S3A config — needed for Spark to write to s3a:// paths
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
)
hadoop_conf.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
hadoop_conf.set("fs.s3a.endpoint.region", "ap-south-1")

# schema must be defined explicitly — inferSchema doesn't work in streaming
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

# kafka delivers value as bytes, cast to string before parsing
parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5-min sliding window (slides every 1 min), watermark drops events >2 min late
metrics = parsed.filter(col("event_type") == "trip_completed") \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("city")) \
    .agg(
        count("event_id").alias("completed_trips"),
        avg("fare").alias("avg_fare")
    ) \
    .withColumn("event_date", date_format(col("window").getField("start"), "yyyy-MM-dd"))

query = metrics.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "s3a://pulsefleet-project-bucket-uditks/streaming-output/") \
    .option("checkpointLocation", "s3a://pulsefleet-project-bucket-uditks/checkpoints/") \
    .partitionBy("event_date") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()

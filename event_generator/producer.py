import json
import random
import time
import io
from datetime import datetime, timezone
from kafka import KafkaProducer
import boto3

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride-events"

S3_BUCKET = "pulsefleet-project-bucket-uditks"
S3_RAW_PREFIX = "raw-events"

# flush to S3 after 500 events or 60s, whichever hits first
BATCH_SIZE = 500
BATCH_INTERVAL_SECONDS = 60

CITIES = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Hyderabad"]
EVENT_TYPES = ["trip_started", "trip_completed", "driver_location"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

s3_client = boto3.client("s3")


def generate_event():
    # generate events with random key-value pairs (synthetic data)
    event_type = random.choice(EVENT_TYPES)
    base = {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "event_type": event_type,
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if event_type == "trip_completed":
        base["fare"] = round(random.uniform(50, 500), 2)
        base["distance_km"] = round(random.uniform(1, 20), 2)
    elif event_type == "driver_location":
        base["lat"] = round(random.uniform(12.8, 13.2), 6)
        base["lon"] = round(random.uniform(80.1, 80.3), 6)
    return base


def flush_batch_to_s3(batch):
    if not batch:
        return

    now = datetime.now(timezone.utc)
    # partitioning so EMR can scan a single day's prefix
    key = (
        f"{S3_RAW_PREFIX}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"events_{now.strftime('%H%M%S')}_{random.randint(1000,9999)}.json"
    )

    # write entire batch into one S3 object instead of one put_object per event
    buffer = io.StringIO()
    for event in batch:
        buffer.write(json.dumps(event) + "\n")

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
    )
    print(f"[S3] Flushed {len(batch)} events → s3://{S3_BUCKET}/{key}")


def main():
    batch = []
    last_flush_time = time.time()

    try:
        while True:
            event = generate_event()

            producer.send(KAFKA_TOPIC, event)
            print(f"[Kafka] {event}")

            batch.append(event)

            time_elapsed = time.time() - last_flush_time
            if len(batch) >= BATCH_SIZE or time_elapsed >= BATCH_INTERVAL_SECONDS:
                flush_batch_to_s3(batch)
                batch = []
                last_flush_time = time.time()

            time.sleep(random.uniform(0.1, 0.5))

    except KeyboardInterrupt:
        print("\nStopping...")
        flush_batch_to_s3(batch)  # flush whatever's left before exiting
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

import json
import random
import time
import io
from datetime import datetime, timezone
from kafka import KafkaProducer
import boto3

# ---- Config ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride-events"

S3_BUCKET = "pulsefleet-project-bucket-uditks"
S3_RAW_PREFIX = "raw-events"

BATCH_SIZE = 500          # flush to S3 after this many events
BATCH_INTERVAL_SECONDS = 60  # or flush after this many seconds, whichever comes first

CITIES = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Hyderabad"]
EVENT_TYPES = ["trip_started", "trip_completed", "driver_location"]

# ---- Clients ----
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

s3_client = boto3.client("s3")


def generate_event():
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
    """Write a batch of events to S3 as a single newline-delimited JSON file."""
    if not batch:
        return

    now = datetime.now(timezone.utc)
    # Partition by date/hour so the EMR batch job can scan efficiently later
    key = (
        f"{S3_RAW_PREFIX}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"events_{now.strftime('%H%M%S')}_{random.randint(1000,9999)}.json"
    )

    buffer = io.StringIO()
    for event in batch:
        buffer.write(json.dumps(event) + "\n")

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
    )
    print(f"[S3] Flushed {len(batch)} events to s3://{S3_BUCKET}/{key}")


def main():
    batch = []
    last_flush_time = time.time()

    try:
        while True:
            event = generate_event()

            # 1. Send to Kafka for the real-time streaming path
            producer.send(KAFKA_TOPIC, event)
            print(f"[Kafka] Sent: {event}")

            # 2. Add to in-memory batch for the S3 archive (batch path)
            batch.append(event)

            # Flush condition: batch size OR time interval, whichever first
            time_elapsed = time.time() - last_flush_time
            if len(batch) >= BATCH_SIZE or time_elapsed >= BATCH_INTERVAL_SECONDS:
                flush_batch_to_s3(batch)
                batch = []
                last_flush_time = time.time()

            time.sleep(random.uniform(0.1, 0.5))

    except KeyboardInterrupt:
        print("\nStopping producer, flushing remaining events...")
        flush_batch_to_s3(batch)   # don't lose the last partial batch
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
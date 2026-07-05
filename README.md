# PulseFleet — Ride Analytics Pipeline

A data engineering project that simulates a ride-hailing event stream and processes it in two ways — real-time for live metrics, and batch for historical aggregates. Built on the Lambda Architecture: same raw data, two processing paths, two different latency tradeoffs.

Kafka handles ingestion, Spark Structured Streaming handles the real-time side, and a Spark job on EMR handles the batch side. Airflow ties the batch lifecycle together. Everything lands on S3.

---

## Architecture

```
                    ┌─────────────────┐
                    │  Event Producer  │
                    │   (Python)       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Kafka       │
                    │  (ride-events)   │
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                              ▼
   ┌───────────────────┐          ┌──────────────────────┐
   │  Spark Structured  │          │   Raw Event Archive   │
   │     Streaming      │          │   (S3 — NDJSON)       │
   │ (5-min sliding     │          └───────────┬───────────┘
   │  window metrics)   │                      │
   └──────────┬─────────┘                      ▼
              │                       ┌──────────────────────┐
              ▼                       │   EMR Batch Job        │
   ┌───────────────────┐              │   (Spark on EMR)       │
   │  S3 — Streaming   │              │  daily revenue,        │
   │     Output        │              │  hourly demand         │
   └──────────┬─────────┘              └───────────┬───────────┘
              │                                      │
              └──────────────────┬───────────────────┘
                                  ▼
                        ┌──────────────────┐
                        │   S3 — Curated    │
                        │   (Parquet)       │
                        └──────────────────┘

        Airflow DAG manages EMR cluster creation,
        job submission, and teardown. Runs at 9 AM UTC daily.
```

---

## How it works

**Producer** — generates `trip_started`, `trip_completed`, and `driver_location` events across 5 Indian cities. Each event goes to Kafka immediately, and also gets batched into S3 as partitioned NDJSON (500 events or 60 seconds, whichever comes first).

**Streaming** — Spark Structured Streaming reads from Kafka continuously, computes 5-minute sliding window aggregates (completed trips + avg fare per city), and writes results to S3. A 2-minute watermark handles late events.

**Batch** — A Spark job on EMR reads the day's raw events from S3 and produces two tables: daily revenue per city, and hourly demand per city. The cluster auto-terminates after the job finishes.

**Orchestration** — An Airflow DAG handles the full EMR lifecycle: spin up cluster → submit step → wait → confirm teardown. Passes `{{ ds }}` as the run date so the job always processes the right partition.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event Streaming | Apache Kafka (Confluent, local via Docker) |
| Stream Processing | Apache Spark Structured Streaming 4.1 |
| Batch Processing | Apache Spark on Amazon EMR 7.13 |
| Storage | Amazon S3 |
| Orchestration | Apache Airflow |
| Language | Python 3.10+ |

---

## S3 Layout

```
pulsefleet-project-bucket-uditks/
├── raw-events/
│   └── year=YYYY/month=MM/day=DD/
│       └── events_HHMMSS_XXXX.json
├── streaming-output/
├── checkpoints/
├── curated/
│   ├── daily_revenue/
│   └── hourly_demand/
├── emr-scripts/
│   └── emr_batch_job.py
└── emr-logs/
```

---

## Repo Structure

```
pulsefleet/
├── event_generator/
│   └── producer.py
├── streaming/
│   └── spark_streaming_job.py
├── batch/
│   └── emr_batch_job.py
├── orchestration/
│   └── dags/
│       └── emr_batch_dag.py
├── infra/
│   └── emr_cluster_config.json
├── docker-compose.yml
└── README.md
```

---

## Running it

### Prerequisites
- Python 3.10+
- Docker Desktop
- AWS CLI configured (`aws configure`)
- Airflow with `apache-airflow-providers-amazon`

### 1. Start Kafka
```bash
docker-compose up -d
docker exec -it pulsefleet-kafka-1 kafka-topics --create \
  --topic ride-events --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 2. Python environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install kafka-python boto3 pyspark
```

### 3. Start the producer
```bash
python event_generator/producer.py
```

### 4. Run the streaming job
```bash
spark-submit streaming/spark_streaming_job.py
```

### 5. Upload the batch script
```bash
aws s3 cp batch/emr_batch_job.py \
  s3://pulsefleet-project-bucket-uditks/emr-scripts/emr_batch_job.py
```

### 6. Trigger the Airflow DAG
Copy `orchestration/dags/emr_batch_dag.py` to your Airflow DAGs folder, set up an `aws_default` connection, and trigger the DAG. It handles everything from cluster creation to teardown.

---

## Pipeline demo

Screenshots and output → [pipeline-demo/PIPELINE_DEMO.md](pipeline-demo/PIPELINE_DEMO.md)

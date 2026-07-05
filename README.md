# PulseFleet — Real-Time & Batch Ride Analytics Pipeline

PulseFleet is a data engineering pipeline that simulates a ride-hailing platform's event stream and processes it two ways at once: in real time for live operational metrics, and in batch for deeper historical analysis. It's built to mirror how large-scale platforms (Uber, Ola, Swiggy-style delivery) actually structure their data infrastructure — combining stream and batch processing in a single system, a pattern commonly known as the **Lambda Architecture**.

The goal of this project is to demonstrate hands-on, end-to-end data engineering skills: event streaming, distributed processing with Spark, cluster-based batch computation on EMR, and orchestration with Airflow — all wired together as one coherent system rather than isolated demos.

---

## Why this project exists

Most portfolio projects show a single tool in isolation — an ETL script here, a dashboard there. PulseFleet is intentionally built to answer a more realistic question: **how do you serve both live and historical views off the same underlying data, without duplicating logic or infrastructure?**

This is a problem every data platform eventually faces, and building it end-to-end — including the operational parts most tutorials skip (cluster lifecycle management, checkpointing, cost control) — was the point.

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

        Orchestration: Airflow DAG manages EMR cluster
        creation, batch job submission, and teardown.
        Runs daily at 9 AM UTC.
```

---

## Data Flow

1. **Event Generation** — A Python producer simulates ride events (`trip_started`, `trip_completed`, `driver_location`) across five Indian cities and publishes them to Kafka. Every event is also written to S3 as partitioned NDJSON (`raw-events/year=/month=/day=/`), forming the source for the batch path.

2. **Real-Time Path** — A Spark Structured Streaming job consumes the Kafka topic continuously, computing 5-minute sliding window metrics (completed trips and average fare per city) and writing results to S3 as they're calculated. A 2-minute watermark handles late-arriving events.

3. **Batch Path** — A Spark job runs on an Amazon EMR cluster, reading the day's raw event archive from S3 and computing daily revenue per city and hourly demand patterns. The cluster spins up, runs the job, and terminates automatically.

4. **Orchestration** — An Airflow DAG manages the entire batch lifecycle: creates the EMR cluster, submits the Spark job as a step, monitors completion, and waits for cluster teardown. Scheduled daily at 9 AM UTC, passes the execution date (`{{ ds }}`) to the job as the run date.

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
│       └── events_HHMMSS_XXXX.json     # NDJSON, flushed by producer
├── streaming-output/                    # JSON written by Spark Streaming
├── checkpoints/                         # Spark Streaming checkpoint state
├── curated/
│   ├── daily_revenue/                   # Parquet, partitioned by trip_date
│   └── hourly_demand/                   # Parquet, partitioned by trip_date
├── emr-scripts/
│   └── emr_batch_job.py                 # Uploaded before running the DAG
└── emr-logs/                            # EMR cluster logs
```

---

## Repository Structure

```
pulsefleet/
├── event_generator/
│   └── producer.py              # Kafka producer + S3 batch writer
├── streaming/
│   └── spark_streaming_job.py   # Spark Structured Streaming job
├── batch/
│   └── emr_batch_job.py         # Spark batch job, runs on EMR
├── orchestration/
│   └── dags/
│       └── emr_batch_dag.py     # Airflow DAG — EMR lifecycle
├── infra/
│   └── emr_cluster_config.json  # EMR cluster definition
├── docker-compose.yml           # Local Kafka + Zookeeper
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker Desktop
- AWS account with CLI configured (`aws configure`)
- Apache Airflow with the `apache-airflow-providers-amazon` package

### 1. Start local Kafka
```bash
docker-compose up -d
docker exec -it pulsefleet-kafka-1 kafka-topics --create \
  --topic ride-events --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 2. Set up Python environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install kafka-python boto3 pyspark
```

### 3. Start the event producer
```bash
python event_generator/producer.py
```

### 4. Run the streaming job
```bash
spark-submit streaming/spark_streaming_job.py
```

### 5. Upload the batch script to S3
```bash
aws s3 cp batch/emr_batch_job.py \
  s3://pulsefleet-project-bucket-uditks/emr-scripts/emr_batch_job.py
```

### 6. Orchestrate with Airflow
Copy `orchestration/dags/emr_batch_dag.py` into your Airflow DAGs folder, configure an `aws_default` connection in the Airflow UI, and trigger the DAG. It will create the EMR cluster, submit the job with the run date, and tear down the cluster on completion.

---

## Design Decisions

- **Why Lambda Architecture?** Real-time metrics answer "what's happening now," batch answers "what happened, in depth." Both views come off the same raw data source — no duplicated ingestion logic.
- **Why auto-terminating EMR clusters?** A persistent cluster running 24/7 for a daily job is wasteful. Spin-up/spin-down per run keeps costs in check and mirrors how cost-conscious teams actually operate EMR.
- **Why dynamic partition overwrite on the batch job?** So re-running the job for the same date replaces only that day's partition without touching any other data. Safe to backfill or retry without duplicates.
- **Why 5-minute sliding windows in streaming?** Granular enough to catch demand spikes in near real-time, without the noise of sub-minute updates.

---

## Author

Built by Udit Krishna S as a personal Data Engineering project.
[LinkedIn](https://linkedin.com/in/udit-krishna) | [GitHub](https://github.com/Udit-Krishna)

---

## Proof of Run

Screenshots and sample output → [pipeline-demo/PIPELINE_DEMO.md](pipeline-demo/PIPELINE_DEMO.md)

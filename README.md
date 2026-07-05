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
   │     Streaming       │          │         (S3)          │
   │ (5-min rolling      │          └───────────┬───────────┘
   │  window metrics)    │                      │
   └──────────┬──────────┘                      ▼
              │                       ┌──────────────────────┐
              ▼                       │   EMR Batch Job        │
   ┌───────────────────┐              │   (Spark on EMR)       │
   │   S3 — Streaming   │              │  Daily revenue, hourly │
   │      Output         │              │  demand aggregates    │
   └──────────┬──────────┘              └───────────┬───────────┘
              │                                      │
              └──────────────────┬───────────────────┘
                                  ▼
                        ┌──────────────────┐
                        │   S3 — Curated    │
                        │   (Parquet)       │
                        └────────┬──────────┘
                                 ▼
                        ┌──────────────────┐
                        │   Streamlit       │
                        │   Dashboard       │
                        └──────────────────┘

        Orchestration: Airflow DAG manages EMR cluster
        creation, batch job execution, and teardown.
```

---

## Data Flow

1. **Event Generation** — A Python producer simulates ride events (`trip_started`, `trip_completed`, `driver_location`) across five Indian cities and publishes them to a Kafka topic. A portion of these events is also archived as raw JSON to S3, forming the source for batch processing.

2. **Real-Time Path** — A Spark Structured Streaming job consumes the Kafka topic continuously, computing rolling 5-minute windowed metrics (completed trips and average fare per city) and writing results to S3 as they're calculated.

3. **Batch Path** — An Apache Spark job runs on an Amazon EMR cluster, reading the full raw event archive from S3 and computing deeper aggregates: daily revenue per city and hourly demand patterns. The cluster is created, runs the job, and terminates automatically to control cost.

4. **Orchestration** — An Airflow DAG manages the entire batch lifecycle: spinning up the EMR cluster, submitting the Spark job as a step, monitoring its progress, and ensuring teardown — scheduled to run daily.

5. **Serving Layer** — A lightweight Streamlit dashboard reads both the streaming output and the curated batch tables from S3, presenting live and historical metrics side by side.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event Streaming | Apache Kafka |
| Stream Processing | Apache Spark (Structured Streaming) |
| Batch Processing | Apache Spark on Amazon EMR |
| Storage | Amazon S3 (raw, streaming output, curated Parquet) |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| Language | Python |

---

## Repository Structure

```
pulsefleet/
├── event_generator/
│   └── producer.py          # Simulates and publishes ride events to Kafka
├── streaming/
│   └── spark_streaming_job.py   # Real-time windowed aggregation
├── batch/
│   └── emr_batch_job.py     # Batch aggregation job run on EMR
├── orchestration/
│   └── dags/
│       └── emr_batch_dag.py # Airflow DAG for EMR lifecycle management
├── dashboard/
│   └── app.py                # Streamlit dashboard
├── infra/
│   └── emr_cluster_config.json
├── docker-compose.yml        # Local Kafka setup
├── requirements.txt
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker Desktop
- AWS account with CLI configured (`aws configure`)
- Apache Airflow (for orchestration steps)

### 1. Set up local Kafka
```bash
docker-compose up -d
docker exec -it pulsefleet-kafka-1 kafka-topics --create \
  --topic ride-events --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 2. Install dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Start the event producer
```bash
python event_generator/producer.py
```

### 4. Run the streaming job
```bash
spark-submit streaming/spark_streaming_job.py
```

### 5. Run the batch job on EMR
```bash
aws emr create-cluster \
  --name "PulseFleet-Batch" \
  --release-label emr-7.0.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --auto-terminate \
  --steps Type=Spark,Name="BatchJob",ActionOnFailure=TERMINATE_CLUSTER,\
Args=[--deploy-mode,cluster,s3://your-bucket/emr-scripts/emr_batch_job.py]
```

### 6. Orchestrate with Airflow
Place `orchestration/dags/emr_batch_dag.py` in your Airflow DAGs folder, configure an `aws_default` connection, and trigger the DAG from the Airflow UI.

### 7. View the dashboard
```bash
streamlit run dashboard/app.py
```

---

## Design Decisions

- **Why Lambda Architecture instead of just streaming or just batch?** Real-time metrics answer "what's happening right now," while batch answers "what happened, in depth." Most production systems need both, and building them together — sharing the same raw data source — better reflects real-world data engineering than either alone.
- **Why auto-terminating EMR clusters?** Cost control. Persistent clusters are expensive and unnecessary for a daily batch job; spin-up/spin-down per run mirrors how many cost-conscious teams actually operate EMR in production.
- **Why 5-minute windows for streaming?** A balance between responsiveness and avoiding noisy, overly granular metrics — adjustable based on operational needs.

---

## Future Improvements

- Migrate local Kafka to Amazon MSK for a fully managed, cloud-native setup
- Add data quality checks (e.g., Great Expectations) on both streaming and batch outputs
- Model the curated layer with dbt for cleaner transformation logic and testing
- Add schema evolution handling for the raw event archive

---

## Author

Built by Udit Krishna S as a personal Data Engineering project to gain hands-on experience with Spark, EMR, and stream processing.
[LinkedIn](https://linkedin.com/in/udit-krishna) | [GitHub](https://github.com/Udit-Krishna)
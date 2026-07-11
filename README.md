# GitHub Events — Real-Time Data Engineering Pipeline

An end-to-end, production-grade streaming data pipeline that ingests public GitHub activity from the live GH Archive stream, handles high-throughput messaging via **managed Aiven Kafka (SSL)**, processes data incrementally with **Spark Structured Streaming** on a **Delta Lake Medallion Architecture**, and serves analytics via **dbt** and **Power BI**.

---

## 🏗️ System Architecture

```mermaid
flowchart TD
    %% Source
    subgraph Ingestion Source
        GH[GH Archive Live Stream] -->|HTTP gzip Stream| Prod[Python Event Producer]
    end

    %% Streaming Message Bus
    subgraph Message Broker
        Prod -->|SSL Client-Cert Auth / Snappy Comp| Kafka(Aiven Managed Kafka)
        subgraph Topics
            direction LR
            T1[github.events.raw]
            T2[github.events.PushEvent]
            T3[github.events.PullRequestEvent]
        end
        Kafka -.-> Topics
    end

    %% Compute and Storage
    subgraph Databricks Lakehouse (Serverless Compute)
        Topics -->|Spark Structured Streaming| Bronze[(Bronze Layer<br>raw_events Delta Table)]
        
        Bronze -->|Post-Ingestion Audit| DQ1{Audit: Schema Drift &<br>Null Event Keys}
        DQ1 -- Pass --> Parse[JSON Parsing & Flattening]
        
        Parse -->|Delta MERGE Deduplication| Silver[(Silver Layer<br>events Delta Table)]
        Silver -->|Delta Constraint| DQ2{Constraint:<br>event_timestamp > 2020}
        
        Silver -->|dbt Models / SQL| Gold[(Gold Layer<br>Star Schema & Aggs)]
        Gold -->|dbt Test| DQ3{Unique & Not-Null<br>Surrogate Keys}
    end

    %% Analytics & Orchestration
    subgraph Serving & Orchestration
        Gold -->|Databricks SQL Connector| BI[Power BI Dashboards]
        Airflow((Apache Airflow<br>Dockerized)) -->|Trigger & Poll REST API| Databricks
        Airflow -->|Execution / Failures| Alerts[Slack / Email Notifications]
    end

    %% Styling
    classDef source fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef broker fill:#231f20,stroke:#fff,stroke-width:2px,color:#fff;
    classDef delta fill:#ff3621,stroke:#fff,stroke-width:2px,color:#fff;
    classDef orchestrator fill:#017cee,stroke:#fff,stroke-width:2px,color:#fff;
    classDef bi fill:#f2c811,stroke:#333,stroke-width:2px;
    
    GH:::source
    Prod:::source
    Kafka:::broker
    Bronze:::delta
    Silver:::delta
    Gold:::delta
    Airflow:::orchestrator
    BI:::bi
```

---

## 🎯 Business Use Case & Problem Statement
Organizations monitoring developer ecosystem health, tracking trending open-source software, or profiling developer activity need to ingest and analyze public GitHub events. 

### The Challenge
GitHub generates roughly **150,000 to 300,000 events per hour** (approx. 50-80 events/sec average, peaking much higher during working hours). 
1. **Schema Evolution**: The JSON payload structure for events varies significantly between 15+ event types (`PushEvent`, `PullRequestEvent`, `WatchEvent`, etc.).
2. **Backpressure**: Periodic API bursts or large hourly archive releases can swamp standard batch consumers.
3. **Exactly-Once Semantics**: To prevent double-counting of metrics (e.g., repository stars, code contributions), duplication from network retries must be eliminated.

---

## ⚡ Performance & Scale Benchmarks
* **Throughput Capacity**: Successfully stress-tested at **167,000+ events per batch (~255,000 total produced messages)**, fully processed from ingestion to Gold aggregation in under **11 minutes**.
* **Latency**: Stream latency from Kafka to Bronze is sub-second. The micro-batch pipeline runs hourly using cost-optimized Serverless triggers, draining the queue in minutes.
* **Storage Footprint**: Raw JSON compression in Delta format reduces storage size by **~75%** compared to uncompressed JSON.

---

## 🛠️ Tech Stack
* **Orchestration**: Apache Airflow (running in Docker Compose)
* **Ingestion/Broker**: Python Confluent-Kafka SDK, Aiven Kafka (fully managed Cloud Apache Kafka with SSL mutual authentication)
* **Processing Engine**: Spark Structured Streaming & PySpark SQL (Databricks Serverless Compute)
* **Storage Layer**: Delta Lake Medallion architecture (Bronze ➔ Silver ➔ Gold)
* **Transformation & Data Quality**: dbt-databricks (for Gold SQL models), Delta Constraints, Spark Assertions
* **BI/Visualizations**: Power BI (DirectQuery over Databricks SQL Warehouse)

---

## 📂 Repository Layout
```directory
github_events_pipeline/
├── .github/workflows/   # CI/CD (Ruff check, SQLFluff check, CD deploy to Databricks)
├── airflow/             # Airflow Docker Compose & DAG definitions
├── certs/               # SSL mutual authentication certificates (gitignored)
├── databricks/          # Databricks Spark notebooks (01_bronze -> 02_silver -> 03_gold)
├── dbt/                 # dbt models, sources, and schema tests
├── producer/            # Python Kafka producer & event routing scripts
├── pyproject.toml       # Ruff formatting rules
├── .sqlfluff            # SQLFluff formatting rules
└── README.md
```

---

## 🚀 Quickstart & Setup Guide

### Phase 1: Environment Variables
Create a `.env` file in the project root:
```env
KAFKA_BOOTSTRAP_SERVERS=your-aiven-kafka-uri:port
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
```

### Phase 2: Aiven Kafka SSL Certs
Download your connection credentials from Aiven console and place them in the `/certs` directory:
- `ca.pem`
- `service.cert`
- `service.key`

### Phase 3: Run Producer (Local Testing)
1. Initialize virtual environment and install packages:
   ```bash
   cd producer
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Test connection and create Kafka topics:
   ```bash
   python test_connection.py
   python create_topics.py
   ```
3. Smoke-test the producer:
   ```bash
   python kafka_producer.py --date 2024-01-15 --hour 12 --limit 500
   ```

### Phase 4: Databricks Credentials & Volumes
1. Convert PEM certificates to JKS keystores for Spark/Java compatibility (see `certs/README.md`).
2. Create a managed Unity Catalog volume `workspace.gh_bronze.pipeline`.
3. Upload the JKS keystore files to the UC Volume: `/Volumes/workspace/gh_bronze/pipeline/certs/`.
4. Create a Databricks Secret Scope named `kafka-secrets` and add:
   - `bootstrap-servers`
   - `ssl-truststore-password`
   - `ssl-keystore-password`

### Phase 5: Airflow Execution
1. Navigate to the airflow directory:
   ```bash
   cd ../airflow
   cp .env.example .env # fill in Databricks Host, Token, Catalog
   docker compose up -d
   ```
2. Open the UI at `http://localhost:8080` (admin/admin), unpause `github_events_pipeline`, and trigger it!

---

## 🛡️ Production Challenges & Engineering Solutions

### 1. Kafka Producer Buffer Overflows (`BufferError`)
* **Problem**: Under high throughput, the Python producer queued messages faster than they could be sent to Aiven, resulting in `BufferError: Local: Queue full` and crash exits.
* **Solution**: Tuning the producer configuration (`queue.buffering.max.messages=200000`, `linger.ms=50`, `compression.type=snappy`) to increase headroom. Implemented custom retry logic in `kafka_producer.py` catching `BufferError`, triggering a blocking `producer.poll(1)` to drain the client queue safely before retrying.

### 2. Silent Ingestion Loss on Flush Timeouts
* **Problem**: If `producer.flush()` timed out, the script previously reported success, causing silent message drops.
* **Solution**: Captured the return value of `producer.flush(timeout=300)`. If remaining messages count is `> 0`, it raises a `RuntimeError` which exits with code 1, forcing Airflow to fail the task and retry.

### 3. Cost-Optimized Compute on Databricks Serverless
* **Problem**: Keeping a Spark cluster running 24/7 to read from Kafka is prohibitively expensive.
* **Solution**: Switched the Structured Streaming query trigger to `.trigger(availableNow=True)`. This spins up Databricks Serverless compute, process-drains all unconsumed offsets from Kafka, saves to Delta, and immediately shuts down the compute, saving **~90% in cloud compute charges**.

### 4. Idempotent Exactly-Once Processing
* **Problem**: Networking issues or retries could cause duplicate events.
* **Solution**:
  - **Bronze**: Spark Structured Streaming manages Kafka offsets inside transaction checkpoints.
  - **Silver**: A `MERGE INTO` operation deduplicates incoming records on `event_id` during upserts.
  - **Gold**: Extended `dbt` tests validate uniqueness Constraints on surrogate keys.

---

## 📈 Dashboard & Monitoring Artifacts

*Placeholder for dashboard screenshots and operational views:*

| Power BI Analytics Dashboard | Databricks Workflows / DAG Runs | Airflow Execution Logs |
| :---: | :---: | :---: |
| ![Power BI Dashboard Placeholder](https://via.placeholder.com/400x250.png?text=Power+BI+GitHub+Activity+Dashboard) | ![Databricks Job Run Placeholder](https://via.placeholder.com/400x250.png?text=Databricks+Job+Run+History) | ![Airflow Logs Placeholder](https://via.placeholder.com/400x250.png?text=Airflow+DAG+Overview) |

---

## 📖 Runbook: Operating the Pipeline

### How to Trigger the Pipeline
The pipeline is scheduled to run **hourly** via Airflow. To trigger it manually:
1. Go to the Airflow UI (`http://localhost:8080`).
2. Locate the `github_events_pipeline` DAG.
3. Click the **Play** button on the top right to trigger a run.

### Checking Pipeline Health
1. **Kafka Health**: Check the status of topics and lags in your Aiven Console.
2. **Airflow Health**: Verify that the DAG blocks are green. 
3. **Data Quality Failures**: If a notebook fails due to a validation failure:
   - Check the Databricks notebook execution outputs.
   - Look for error messages prefixed with: `DATA QUALITY FAILURE: ...`

### Recovering from Failures
- **Airflow Task Failed**: Hover over the failed task and check the log. If the error was temporary (e.g. Databricks network timeout), click the task and click **Clear** to retry it.
- **Databricks Notebook Failure**: Go to Databricks Workspace -> Jobs -> Runs. Find the failed run, fix the configuration/secrets, and click **Repair Run** to continue execution from the failed task.

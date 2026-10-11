# Enterprise Real-Time Streaming Data Platform & Hybrid Recommender System

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Kafka-3.6-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Redis-Feature%20Store-DC382D?style=for-the-badge&logo=redis&logoColor=white"/>
  <img src="https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi&logoColor=white"/>
  <img src="https://img.shields.io/badge/PyTorch-NCF%20(CUDA)-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/dbt-Core%201.9-FF694B?style=for-the-badge&logo=dbt&logoColor=white"/>
  <img src="https://img.shields.io/badge/Kubernetes-K8s-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker%20Compose-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
</p>

---

## 1. Executive Summary & Overview

This project is an **Enterprise-Grade Real-Time Streaming Data Platform & MLOps Hybrid Recommendation System** designed for high-throughput e-commerce environments. It addresses **Cart Abandonment, Cross-Selling, and Personalized Discovery** by unifying **Association Rule Mining (FP-Growth)** and **Deep Learning (PyTorch Neural Collaborative Filtering - NCF)** under a sub-5ms low-latency serving architecture.

### Architectural Evolution
Originally developed as a local batch proof-of-concept running on static CSV files and dbt PostgreSQL, the platform has been re-architected into a **Cloud-Native, Event-Driven Lambda/Lakehouse Architecture**:
- **100% Real-World Data**: Ingests **1,067,371 transactions** from the Kaggle UCI Online Retail II dataset, partitioning clean events into Apache Parquet lakehouse storage.
- **Reverse Funnel Augmentation**: Reconstructs customer browsing paths (`view` $\rightarrow$ `add_to_cart` $\rightarrow$ `purchase`) with metadata (`session_id`, `device_type`), streaming at **20,480 events/second** into **Apache Kafka**.
- **Real-Time Sliding Cart Window**: Stream consumer tracks active shopping carts in **Redis Hashes** with a 3600-second TTL.
- **Sub-5ms Inference Gateway**: **FastAPI** queries Redis in $O(1)$ time complexity for co-purchase rules and precomputed user embeddings, backed by a resilient in-memory fallback.
- **Continuous Orchestration & MLOps**: **Apache Airflow** schedules daily Lakehouse ELT with data quality gates and triggers weekly model retraining with MLflow versioning.

---

## 2. High-Level System Architecture

<p align="center">
  <img src="imgs/architecture.png" alt="Enterprise Hybrid Recommender Platform Architecture" width="100%"/>
</p>

### 🔄 End-to-End Data Flow

The distributed pipeline operates across 5 decoupled tiers:

1. **Client & Presentation Layer**:
   - Customers interact with the **Streamlit Web UI** (`:8501`) to browse products, add items to their session cart, and inspect recommendation outputs.
   - The UI polls recommendations from the **FastAPI Gateway** (`:8000`) and simultaneously emits interaction telemetry.

2. **Real-Time Event Streaming & Serving (Sub-5ms)**:
   - High-velocity clickstream events (`view`, `add_to_cart`, `purchase`) are ingested into **Apache Kafka** topic `ecom.events.raw`, keyed by `user_id` for deterministic partition ordering.
   - The **Cart Session Consumer** microservice consumes stream records and maintains live carts inside **Redis** (`cart:session:{session_id}`) with an automatic sliding session TTL.
   - The FastAPI backend performs atomic $O(1)$ key lookups against Redis to fetch active cart rules (`rules:fpgrowth:{item}`) and user embeddings (`ncf:user:{user_id}`).

3. **Data Lakehouse Storage (Medallion Architecture)**:
   - **Kafka Connect** writes micro-batched JSON events to **MinIO Object Storage** (S3-compatible) in compressed Apache Parquet format (**Bronze Layer**).
   - Serves as the Single Source of Truth (SSOT) for offline analytical processing and audit logging.

4. **Continuous MLOps & Orchestration**:
   - **Apache Airflow** runs automated workflows:
     - `daily_lakehouse_elt_pipeline`: Executes **dbt** / **Spark** transformations to clean staging logs, construct Gold Marts (`transaction_fpgrowth` and `user_item_dl`), and enforce Data Quality Gates.
     - `retrain_hybrid_recommender_pipeline`: Mines association rules via **FP-Growth** (386 rules mined in 2.90s) and trains the **PyTorch NCF** neural network on GPU/CUDA, logging checkpoints to **MLflow** and syncing $O(1)$ lookup dictionaries into Redis.

5. **Cloud-Native Deployment**:
   - Packaged with **Kubernetes manifests** and **Kustomize**, including Strimzi Kafka cluster operator, Redis StatefulSet, Airflow deployments, and Horizontal Pod Autoscaling (HPA) for FastAPI.

---

## 3. Performance & Benchmark Metrics

All API endpoints and training routines were benchmarked on a local production-grade test suite:

```text
========================================================================================
>> API SERVING BENCHMARKS (FastAPI TestClient & Redis Feature Store)
========================================================================================
- GET  /health                        -> Status: 200 OK | Latency: 3.56 ms (Healthy)
- POST /cart/add                      -> Status: 200 OK | Latency: 1.69 ms
- GET  /cart/{session_id}             -> Status: 200 OK | Latency: 1.25 ms
- GET  /recommend/cart/{session_id}   -> Status: 200 OK | Latency: 2.28 ms (Triggers FP-Growth co-purchase)
- GET  /recommend/user/17841          -> Status: 200 OK | Latency: 14.36 ms (PyTorch NCF Inference)
- POST /recommend/hybrid              -> Status: 200 OK | Latency: 3.57 ms (Dual-signal fusion)

========================================================================================
>> DATA PIPELINE & MODEL TRAINING BENCHMARKS
========================================================================================
- Kafka Stream Replayer Throughput   : 20,480 events / second (6,763 events in 0.33s)
- FP-Growth Association Rule Mining   : 33,897 baskets scanned -> 386 rules generated in 2.90s
- PyTorch NCF Training (CUDA)        : 1,481,040 samples (493K positive + 2:1 neg samples) in 36.05s
- Lakehouse Parquet Compression Ratio: 90.46 MB CSV -> 5.17 MB Parquet (17.5x compression)
```

---

## 4. Repository Structure

```text
recommendation-systems-rule-base/
├── airflow/                                    # Apache Airflow Orchestration & Workflows
│   ├── dags/
│   │   ├── lakehouse_elt_dag.py               # Daily ELT DAG: dbt staging, marts & data quality assertions
│   │   └── retrain_recommender_dag.py         # Weekly MLOps DAG: retrain FP-Growth & NCF, sync Redis
│   └── docker-compose-airflow.yml             # Standalone Airflow 2.8 stack (Webserver, Scheduler, Postgres)
├── backend/                                    # Production FastAPI Serving Gateway
│   ├── app/
│   │   ├── core/config.py                     # Environment settings, Redis pool configuration, CORS
│   │   ├── models/schemas.py                  # Pydantic schemas for requests, responses & carts
│   │   ├── services/recommender.py            # Hybrid recommendation engine (Redis + In-Memory fallback)
│   │   └── main.py                            # FastAPI entrypoint, lifespan handlers, REST endpoints
│   ├── Dockerfile                             # Multi-stage container build
│   └── Dockerfile.backend                     # Container specification for backend service
├── data/                                       # Local Lakehouse & Model Artifact Storage
│   ├── raw_kaggle/                            # Download directory for Kaggle dataset (.gitkeep)
│   ├── lakehouse/                             # Partitioned Parquet lakehouse (real_transactions.parquet)
│   └── models/                                # Serialized models (fpgrowth_rules.json, rules.csv)
├── data_pipeline/                              # Real Data Ingestion & Stream Replayer
│   ├── kaggle_stream_producer.py              # Kaggle ingestion, reverse funnel augmentation & Kafka producer
│   ├── ingest_kaggle_data.py                  # Kaggle API downloader and cleaner
│   └── config.py                              # Data pipeline configuration
├── dbt_project/                                # dbt Data Warehouse Models
│   ├── models/
│   │   ├── staging/stg_online_retail.sql      # Raw log cleansing and schema casting
│   │   └── marts/
│   │       ├── transaction_fpgrowth.sql       # Basket-level transaction aggregation
│   │       └── user_item_dl.sql               # User-item distinct interaction matrix
│   └── dbt_project.yml
├── docs/                                       # Technical Documentation & Automation
│   └── generate_architecture.py               # Python Diagrams-as-Code script to generate architecture.png
├── imgs/                                       # Architecture & Documentation Diagrams
│   └── architecture.png                       # High-level architecture diagram (Diagrams-as-Code)
├── k8s/                                        # Cloud-Native Kubernetes Manifests
│   ├── kafka/kafka-cluster.yaml               # Strimzi Kafka 3-broker cluster & KafkaTopic CRDs
│   ├── redis/redis-deployment.yaml            # Redis Feature Store Deployment & Service
│   ├── backend/backend-deployment.yaml        # FastAPI Deployment, Service & HPA Autoscaler
│   ├── airflow/airflow-deployment.yaml        # Airflow Webserver, Scheduler & Postgres StatefulSet
│   ├── ui/ui-deployment.yaml                  # Streamlit Web UI Deployment & Service
│   ├── ingress.yaml                           # NGINX Ingress Controller routing rules
│   └── kustomization.yaml                     # Kustomize root manifest
├── mlops/                                      # MLOps Pipelines & Feature Store Sync
│   ├── train_fpgrowth.py                      # Frequent pattern mining with mlxtend FP-Growth
│   ├── train_ncf.py                           # PyTorch Neural Collaborative Filtering trainer (CUDA)
│   └── sync_redis_store.py                    # Atomic pipeline synchronizer to Redis
├── models/
│   └── ncf_model.pt                           # Serialized PyTorch NCF neural checkpoint (3.08 MB)
├── streaming/                                  # Real-Time Stream Consumers
│   ├── consumers/cart_session_consumer.py     # Kafka consumer aggregating active carts into Redis
│   └── config.py                              # Centralized streaming parameters
├── ui/                                         # Frontend Interactive Dashboard
│   ├── streamlit_app.py                       # Live Cart & Hybrid Suggestions Streamlit application
│   └── style.css                              # Custom CSS styling
├── docker-compose.yml                          # Core infrastructure stack (Kafka, Redis, Postgres, MinIO)
└── requirements.txt                            # Pinned production Python dependencies
```

---

## 5. Technology Stack

| Domain | Technologies | Purpose in Platform |
| :--- | :--- | :--- |
| **Event Streaming** | Apache Kafka, Kafka Connect | Ingests live user interaction events (`view`, `cart`, `buy`) with partition ordering. |
| **Low-Latency Feature Store** | Redis | In-memory storage for active carts ($TTL = 3600s$), FP-Growth rules, and user embeddings. |
| **API Serving Gateway** | FastAPI, Uvicorn, Pydantic | Sub-5ms REST endpoints for live cart recommendations, health checks, and fallback serving. |
| **Deep Learning & Mining** | PyTorch, mlxtend | Trains NCF neural recommendation model on GPU/CUDA and mines co-purchase association rules. |
| **Data Lakehouse** | MinIO, Apache Parquet | S3-compatible object storage preserving raw transaction logs in partitioned columnar format. |
| **Data Transformations** | dbt (Data Build Tool), DuckDB | SQL-based transformations creating staging views and curated marts (`transaction_fpgrowth`). |
| **Workflow Orchestration** | Apache Airflow | Schedules daily Lakehouse ELT and weekly retraining pipelines with health checks. |
| **MLOps & Registry** | MLflow | Tracks model hyperparameters, metrics (BCE loss), and artifact checkpoints. |
| **Container & Cloud-Native** | Docker Compose, Kubernetes, Kustomize | Portable local dev environment and production-ready K8s manifests with HPA. |
| **Frontend UI** | Streamlit | Interactive e-commerce user simulation and recommendation inspection dashboard. |

---

## 6. Quickstart Guide

### Prerequisites
- Python 3.10+ (Recommended: Python 3.11)
- Git
- Docker & Docker Compose (Optional for local development; system has automated In-Memory Fallbacks)

### Step 1: Clone Repository & Install Dependencies
```bash
git clone https://github.com/your-username/recommendation-systems-rule-base.git
cd recommendation-systems-rule-base

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Download Real Data & Build Lakehouse Parquet
Download the real Kaggle dataset (`mashlyn/online-retail-ii-uci`) or use the bundled fallback data:
```bash
# Downloads from Kaggle or processes data/online_retail.csv, generating data/lakehouse/real_transactions.parquet
python data_pipeline/ingest_kaggle_data.py --source local
```

### Step 3: Run with Docker Compose (Full Stack)
Start the complete infrastructure (Zookeeper, Kafka, Kafka-UI, Redis, MinIO, PostgreSQL, Backend API, Cart Consumer):
```bash
docker compose up -d
```
- **FastAPI Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Streamlit Web UI**: [http://localhost:8501](http://localhost:8501)
- **Kafka UI**: [http://localhost:8085](http://localhost:8085)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`)

### Step 4: Run in Standalone Mode (No Docker Required)
The platform includes an **In-Memory Fallback Engine**, allowing immediate testing on any machine without starting Docker:

1. **Start Backend API Gateway**:
   ```bash
   uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **Start Streamlit Dashboard**:
   ```bash
   streamlit run ui/streamlit_app.py
   ```

3. **Replay Real-Time Events (Simulation)**:
   ```bash
   # Replays 50 real transactions as a live clickstream at 20,000+ events/sec
   python data_pipeline/kaggle_stream_producer.py --max-invoices 50
   ```

---

## 7. MLOps & Model Retraining

### 1. Mine Association Rules with FP-Growth
```bash
python mlops/train_fpgrowth.py \
  --data-path data/lakehouse/real_transactions.parquet \
  --min-support 0.01 \
  --min-confidence 0.20 \
  --min-lift 1.0
```
*Output*: Generates [data/models/fpgrowth_rules.json](data/models/fpgrowth_rules.json) containing 386 rules formatted for $O(1)$ key lookup.

### 2. Retrain PyTorch NCF Deep Learning Model
```bash
python mlops/train_ncf.py \
  --data-path data/lakehouse/real_transactions.parquet \
  --epochs 3 \
  --batch-size 2048 \
  --emb-size 64 \
  --output-model models/ncf_model.pt
```

### 3. Synchronize Artifacts to Redis Feature Store
```bash
python mlops/sync_redis_store.py \
  --redis-host localhost \
  --redis-port 6379 \
  --max-ncf-users 2000
```

---

## 8. Apache Airflow Orchestration

To run the automated DAGs, start the dedicated Airflow stack:
```bash
docker compose -f airflow/docker-compose-airflow.yml up -d
```
Access the Airflow UI at [http://localhost:8080](http://localhost:8080) (`admin` / `admin`).

1. **`daily_lakehouse_elt_pipeline`**:
   - `extract_and_validate_raw` $\rightarrow$ `run_dbt_staging_transformations` $\rightarrow$ `run_dbt_marts_transformations` $\rightarrow$ `validate_data_quality` $\rightarrow$ `export_gold_marts_for_ml`.
2. **`retrain_hybrid_recommender_pipeline`**:
   - `check_lakehouse_freshness` $\rightarrow$ [`mine_fpgrowth_rules`, `retrain_ncf_model`] $\rightarrow$ `sync_to_redis_feature_store` $\rightarrow$ `verify_serving_health`.

---

## 9. API Reference

### Health Check
```http
GET /health
```
```json
{
  "status": "healthy",
  "redis_connected": true,
  "ncf_model_loaded": true,
  "timestamp": 1791738624.22
}
```

### Add Item to Active Session Cart
```http
POST /cart/add
Content-Type: application/json

{
  "session_id": "sess_user_99",
  "user_id": 17841,
  "item_id": "22423",
  "item_name": "regency cakestand 3 tier",
  "price": 12.75,
  "quantity": 1
}
```

### Get Real-Time Cart Recommendations (FP-Growth)
```http
GET /recommend/cart/{session_id}?top_k=3
```
```json
{
  "session_id": "sess_user_99",
  "cart_items": ["regency cakestand 3 tier"],
  "recommendations": [
    {
      "item_name": "green regency teacup and saucer",
      "score": 0.8125,
      "type": "fp_growth_copurchase",
      "lift": 5.42,
      "triggered_by": "regency cakestand 3 tier"
    }
  ]
}
```

### Get Personalized User Recommendations (PyTorch NCF)
```http
GET /recommend/user/17841?top_k=3
```
```json
{
  "user_id": 17841,
  "recommendations": [
    {
      "item_name": "white hanging heart t-light holder",
      "score": 0.9262,
      "type": "ncf_personalized",
      "lift": null,
      "triggered_by": null
    }
  ]
}
```

### Hybrid Recommendations (Cart Signals + User Profile)
```http
POST /recommend/hybrid
Content-Type: application/json

{
  "user_id": 17841,
  "session_id": "sess_user_99",
  "top_k": 5
}
```

---

## 10. Kubernetes Deployment

To deploy the entire platform onto an existing Kubernetes cluster:
```bash
# Apply entire stack via Kustomize
kubectl apply -k k8s/

# Verify running pods
kubectl get pods -n recsys-platform
```

---

## 11. Diagrams-as-Code

The high-level architecture diagram is programmatically maintained using Python's `diagrams` library:
```bash
python docs/generate_architecture.py
```
This updates [imgs/architecture.png](imgs/architecture.png) and ensures system documentation never drifts from codebase reality.

---

## 12. License & Acknowledgements

- **License**: MIT License
- **Dataset**: [Kaggle UCI Online Retail II](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)
- Developed by the Data Engineering & MLOps Architecture Team.
# filepath: docs/generate_architecture.py
"""
Senior Cloud Architect & Technical Lead:
Diagrams-as-Code Automation for Enterprise Hybrid Recommender Platform.

Generates High-Level System Architecture Diagram:
- Client & UI Layer (Streamlit & Users)
- Low-latency Serving Layer (FastAPI & Redis Feature Store)
- Event Streaming Fabric (Apache Kafka & Kafka Connect)
- Lakehouse & Batch Layer (MinIO S3 & dbt / Spark)
- MLOps & Orchestration Layer (Apache Airflow, PyTorch NCF & FP-Growth, MLflow)
- Cloud-Native Deployment on Kubernetes
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# Automatically inject local Graphviz binary if present
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_GRAPHVIZ = PROJECT_ROOT / "tools" / "graphviz" / "Graphviz" / "bin"
if LOCAL_GRAPHVIZ.exists():
    os.environ["PATH"] = str(LOCAL_GRAPHVIZ.resolve()) + os.pathsep + os.environ.get("PATH", "")

# Diagrams library imports
from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.k8s.compute import Pod
from diagrams.onprem.analytics import Dbt, Spark
from diagrams.onprem.client import User, Users
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.mlops import Mlflow
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow
from diagrams.programming.framework import FastAPI
from diagrams.programming.language import Python


def generate_architecture_diagram(output_dir: Path) -> Path:
    """Renders high-level architecture diagram and saves as PNG."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target_basename = output_dir / "architecture"

    # Global Graph Styling
    graph_attr = {
        "fontsize": "28",
        "fontcolor": "#1E293B",
        "bgcolor": "#F8FAFC",
        "pad": "0.6",
        "splines": "spline",
        "nodesep": "0.8",
        "ranksep": "1.1",
    }

    node_attr = {
        "fontsize": "12",
        "fontname": "Helvetica",
    }

    edge_attr = {
        "fontsize": "10",
        "fontname": "Helvetica",
    }

    with Diagram(
        name="Enterprise Hybrid Recommender Streaming Platform Architecture",
        filename=str(target_basename),
        outformat="png",
        show=False,
        direction="LR",
        graph_attr=graph_attr,
        node_attr=node_attr,
        edge_attr=edge_attr,
    ):
        # 1. CLIENT LAYER
        with Cluster("Client & Presentation Layer", graph_attr={"bgcolor": "#EEF2FF", "pencolor": "#6366F1"}):
            user = User("Shopper / Client\n(Browser / Mobile)")
            streamlit_ui = Python("Streamlit UI\n(:8501 Dashboard)")

        # 2. CLOUD-NATIVE KUBERNETES DEPLOYMENT
        with Cluster("Kubernetes Production Cluster (recsys-platform)", graph_attr={"bgcolor": "#FFFFFF", "pencolor": "#3B82F6"}):

            # A. SERVING LAYER
            with Cluster("Sub-5ms Serving Gateway", graph_attr={"bgcolor": "#EFF6FF", "pencolor": "#60A5FA"}):
                fastapi_app = FastAPI("FastAPI Gateway\n(:8000)")
                redis_store = Redis("Redis Feature Store (:6379)\n(Active Cart | Rules | Embeddings)")

            # B. INGESTION & REAL-TIME STREAMING
            with Cluster("Real-Time Event Streaming", graph_attr={"bgcolor": "#FEF3C7", "pencolor": "#F59E0B"}):
                kafka_broker = Kafka("Apache Kafka\n(Topic: ecom.events.raw)")
                cart_consumer = Pod("Cart Session Consumer\n(Session Windowing)")
                kafka_connect = Kafka("Kafka Connect\n(S3 / Parquet Sink)")

            # C. LAKEHOUSE & BATCH PROCESSING
            with Cluster("Data Lakehouse & Batch Layer", graph_attr={"bgcolor": "#ECFDF5", "pencolor": "#10B981"}):
                minio_storage = S3("MinIO Lakehouse (S3)\n(Bronze Partitioned Parquet)")
                dbt_transform = Dbt("dbt Transformations\n(Staging & Gold Marts)")
                spark_engine = Spark("Apache Spark\n(Batch Aggregations)")

            # D. ORCHESTRATION & MLOPS
            with Cluster("Continuous MLOps & Orchestration", graph_attr={"bgcolor": "#FAF5FF", "pencolor": "#A855F7"}):
                airflow_orch = Airflow("Apache Airflow\n(DAG Scheduler & Triggerer)")
                fpgrowth_miner = Pod("FP-Growth Miner\n(Market Basket Rules)")
                ncf_trainer = Pod("PyTorch NCF Training\n(CUDA Deep Learning)")
                mlflow_registry = Mlflow("MLflow Model Registry\n(Version Control & Artifacts)")

        # ----------------------------------------------------
        # DATA FLOW CONNECTIONS & COLOR CODED EDGES
        # ----------------------------------------------------

        # Luồng tương tác người dùng (Client Interaction)
        user >> Edge(label="1. Browse / Add to Cart", color="#4F46E5", style="bold") >> streamlit_ui
        streamlit_ui >> Edge(label="2. Real-time Events\n(view, cart, buy)", color="#D97706", style="bold") >> kafka_broker
        streamlit_ui >> Edge(label="Query Recs (<5ms)", color="#2563EB", style="dashed") >> fastapi_app

        # Luồng Real-time Serving (Sub-5ms)
        kafka_broker >> Edge(label="Stream Events", color="#D97706") >> cart_consumer
        cart_consumer >> Edge(label="HSET active_cart\n(TTL 3600s)", color="#DC2626", style="bold") >> redis_store
        fastapi_app >> Edge(label="O(1) Feature Lookup", color="#2563EB", style="bold") >> redis_store

        # Luồng Lakehouse Ingestion
        kafka_broker >> Edge(label="Micro-batch Dump", color="#059669") >> kafka_connect
        kafka_connect >> Edge(label="Write Raw Parquet", color="#059669", style="bold") >> minio_storage

        # Luồng Batch ELT & Data Quality
        airflow_orch >> Edge(label="Trigger Daily ELT", color="#7C3AED", style="dotted") >> dbt_transform
        minio_storage >> Edge(label="Read Bronze", color="#059669") >> dbt_transform
        dbt_transform >> Edge(label="Curated Marts", color="#059669") >> spark_engine
        spark_engine >> Edge(label="Gold Tables", color="#059669") >> minio_storage

        # Luồng MLOps & Retraining
        airflow_orch >> Edge(label="Trigger Weekly Retrain", color="#7C3AED", style="bold") >> fpgrowth_miner
        airflow_orch >> Edge(label="Trigger NCF Retrain", color="#7C3AED", style="bold") >> ncf_trainer

        minio_storage >> Edge(label="Read Baskets", color="#4B5563", style="dashed") >> fpgrowth_miner
        minio_storage >> Edge(label="Read Interactions", color="#4B5563", style="dashed") >> ncf_trainer

        ncf_trainer >> Edge(label="Log Checkpoints", color="#9333EA") >> mlflow_registry

        # Đồng bộ Feature Store (Sync to Redis)
        fpgrowth_miner >> Edge(label="Sync Association Rules", color="#DC2626", style="bold") >> redis_store
        ncf_trainer >> Edge(label="Sync User Embeddings", color="#DC2626", style="bold") >> redis_store

    generated_png = output_dir / "architecture.png"
    print(f"Architecture diagram generated successfully at: {generated_png}")

    # Copy to imgs/ for README referencing
    imgs_png = PROJECT_ROOT / "imgs" / "architecture.png"
    imgs_png.parent.mkdir(parents=True, exist_ok=True)

    if generated_png.exists():
        shutil.copy(generated_png, imgs_png)
        print(f"Copied architecture diagram to: {imgs_png}")

    return generated_png


if __name__ == "__main__":
    docs_dir = PROJECT_ROOT / "docs"
    generate_architecture_diagram(docs_dir)

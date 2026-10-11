# filepath: data_pipeline/config.py
"""
Data Pipeline Configuration:
Shared parameters for Ingestion, Funnel Augmentation, and Lakehouse Storage.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

# Kaggle Dataset
KAGGLE_DATASET_SLUG = "mashlyn/online-retail-ii-uci"
RAW_DATA_DIR = ROOT_DIR / "data" / "raw_kaggle"
FALLBACK_CSV_PATH = ROOT_DIR / "data" / "online_retail.csv"

# Lakehouse Storage
LAKEHOUSE_DIR = ROOT_DIR / "data" / "lakehouse"
PARQUET_TARGET_FILE = LAKEHOUSE_DIR / "real_transactions.parquet"
PARTITIONS_DIR = LAKEHOUSE_DIR / "transactions_partitioned"

# Kafka Parameters
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_TOPIC", "ecom.events.raw")

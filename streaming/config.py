# filepath: streaming/config.py
"""
Streaming Configuration:
Centralized parameters for Kafka Stream Consumers, Session Aggregators, and Redis Feature Store.
"""

from __future__ import annotations

import os
from pathlib import Path

# Project paths
ROOT_DIR = Path(__file__).resolve().parent.parent

# Kafka Broker Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "ecom.events.raw")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "recsys.cart.session.group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
KAFKA_ENABLE_AUTO_COMMIT = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true"

# Redis Feature Store Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Session Windowing & Caching TTLs
ACTIVE_CART_TTL_SECONDS = int(os.getenv("ACTIVE_CART_TTL_SECONDS", "3600"))  # 1 hour sliding window
USER_RECENT_VIEWS_LIMIT = int(os.getenv("USER_RECENT_VIEWS_LIMIT", "20"))
RULES_CACHE_TTL_SECONDS = int(os.getenv("RULES_CACHE_TTL_SECONDS", "86400"))  # 24 hours
USER_EMBEDDINGS_TTL_SECONDS = int(os.getenv("USER_EMBEDDINGS_TTL_SECONDS", "86400"))

# Processing Parameters
BATCH_FLUSH_INTERVAL_SEC = float(os.getenv("BATCH_FLUSH_INTERVAL_SEC", "1.0"))
MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", "500"))

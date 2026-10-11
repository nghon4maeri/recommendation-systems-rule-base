# filepath: backend/app/core/config.py
"""
Production Configuration for Backend Gateway.
Loads settings from environment variables with sensible defaults.
"""

from __future__ import annotations

import os
from pathlib import Path
from pydantic import BaseModel


class Settings(BaseModel):
    PROJECT_NAME: str = "Real-Time Streaming Recommender API"
    VERSION: str = "2.0.0"
    API_V1_STR: str = "/api/v1"

    # Redis Feature Store Config
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD", None)

    # Database Config
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:abc123password@localhost:5432/fp_growth_mba",
    )

    # Kafka Broker Config
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_RAW_TOPIC: str = os.getenv("KAFKA_TOPIC", "ecom.events.raw")

    # ML Model Artifact Paths
    ROOT_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent
    NCF_MODEL_PATH: Path = ROOT_DIR / "models" / "ncf_model.pt"
    RULES_JSON_PATH: Path = ROOT_DIR / "data" / "models" / "fpgrowth_rules.json"
    RULES_CSV_PATH: Path = ROOT_DIR / "data" / "rules.csv"


settings = Settings()

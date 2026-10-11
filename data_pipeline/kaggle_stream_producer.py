"""
data_pipeline/kaggle_stream_producer.py
Senior Data Engineer Production Module:
End-to-End Real Kaggle Ingestion, Behavior Funnel Augmentation, and Kafka Stream Producer.

Features:
1. Fetch & Clean: Downloads 'Online Retail II' from Kaggle (or uses local dataset), filters invalid rows.
2. Behavioral Reverse-Augmentation:
   - For every real invoice (purchase event), back-propagates realistic browsing funnels:
     * 'view' events (with browsing duration and abandoned browse items).
     * 'add_to_cart' events prior to checkout.
     * Enriched metadata: session_id, device_type (mobile/desktop/tablet), event_id, timestamp.
3. Kafka Streaming Engine:
   - Publishes JSON events to Kafka topic with key=user_id for partition ordering.
   - Configurable playback rate (events/sec or real-time simulation).
   - Dry-run mode for local development without live Kafka brokers.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("KaggleStreamProducer")


@dataclass(frozen=True)
class EnrichedEvent:
    event_id: str
    session_id: str
    user_id: int
    item_id: str
    item_name: str
    category: str
    price: float
    quantity: int
    event_type: str  # 'view', 'add_to_cart', 'purchase'
    device_type: str  # 'mobile', 'desktop', 'tablet'
    country: str
    timestamp: str  # ISO-8601
    event_date: str  # YYYY-MM-DD for partitioning


class KaggleDataLoader:
    """Manages downloading, caching, and cleaning real Kaggle transaction data."""

    KAGGLE_DATASET = "mashlyn/online-retail-ii-uci"

    def __init__(self, raw_dir: Path, fallback_csv: Path):
        self.raw_dir = raw_dir
        self.fallback_csv = fallback_csv
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def get_raw_filepath(self) -> Path:
        """Finds cached Kaggle CSV or attempts automated download."""
        # 1. Check existing downloaded files in raw_dir
        csv_candidates = list(self.raw_dir.glob("*.csv"))
        if csv_candidates:
            target = max(csv_candidates, key=lambda f: f.stat().st_size)
            logger.info(f"Using cached raw Kaggle dataset: {target.name} ({target.stat().st_size / 1024 / 1024:.2f} MB)")
            return target

        # 2. Try automated download via Kaggle CLI
        logger.info(f"Downloading dataset '{self.KAGGLE_DATASET}' from Kaggle...")
        try:
            cmd = ["kaggle", "datasets", "download", "-d", self.KAGGLE_DATASET, "-p", str(self.raw_dir), "--unzip"]
            res = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Kaggle download successful: {res.stdout.strip()}")
            csv_candidates = list(self.raw_dir.glob("*.csv"))
            if csv_candidates:
                return max(csv_candidates, key=lambda f: f.stat().st_size)
        except Exception as e:
            logger.warning(f"Kaggle CLI download failed or not configured: {e}")

        # 3. Fallback to existing local repository data
        if self.fallback_csv.exists():
            logger.info(f"Falling back to local repository file: {self.fallback_csv} ({self.fallback_csv.stat().st_size / 1024 / 1024:.2f} MB)")
            return self.fallback_csv

        raise FileNotFoundError("Could not find or download real dataset. Please verify Kaggle credentials or data/online_retail.csv.")

    def load_clean_data(self) -> pd.DataFrame:
        raw_path = self.get_raw_filepath()
        logger.info(f"Reading dataset: {raw_path}")

        try:
            df = pd.read_csv(raw_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(raw_path, encoding="ISO-8859-1")

        # Standardize columns
        df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]
        rename_map = {
            "invoice": "invoiceno", "invoiceno": "invoiceno",
            "stockcode": "stockcode",
            "description": "description",
            "quantity": "quantity",
            "invoicedate": "invoicedate",
            "price": "unitprice", "unitprice": "unitprice",
            "customer id": "customerid", "customerid": "customerid",
            "country": "country"
        }
        df = df.rename(columns={c: rename_map[c] for c in df.columns if c in rename_map})

        # Data Cleaning: filter out nulls, negative prices/quantities, and cancellations
        initial_len = len(df)
        df = df.dropna(subset=["invoiceno", "description", "customerid"])
        df = df[df["quantity"] > 0]
        df = df[df["unitprice"] > 0]
        df = df[~df["invoiceno"].astype(str).str.startswith("C")]

        df["customerid"] = df["customerid"].astype(int)
        df["invoicedate"] = pd.to_datetime(df["invoicedate"])
        df["description"] = df["description"].astype(str).str.strip().str.lower()
        df["stockcode"] = df["stockcode"].astype(str).str.strip()

        df = df.sort_values("invoicedate").reset_index(drop=True)
        logger.info(f"Cleaned {len(df):,} valid rows from {initial_len:,} raw rows ({len(df)/initial_len*100:.1f}% retention).")
        return df


class BehaviorFunnelAugmentor:
    """
    Interpolates customer behavior backwards from final purchase orders:
    - Generates realistic 'view' and 'add_to_cart' actions.
    - Adds abandoned/browse items with drop-off funnel dynamics.
    - Enriches with realistic session and device context.
    """

    DEVICE_TYPES = ["mobile", "desktop", "tablet"]
    DEVICE_PROBS = [0.65, 0.30, 0.05]

    CATEGORY_KEYWORDS = {
        "Bag & Luggage": ["bag", "tote", "handbag", "shopper", "luggage", "backpack"],
        "Home & Kitchen": ["mug", "cup", "plate", "bowl", "kitchen", "jar", "glass", "bottle", "tea", "coffee"],
        "Stationery & Crafts": ["pencil", "pen", "notebook", "sticker", "card", "paper", "eraser", "ruler"],
        "Decor & Lighting": ["candle", "light", "garland", "frame", "mirror", "clock", "cushion", "lantern"],
        "Apparel & Accessories": ["shirt", "dress", "scarf", "glove", "hat", "sock", "jewelry", "necklace"],
    }

    def __init__(self, catalog_df: pd.DataFrame, seed: int = 42):
        self.catalog = catalog_df[["stockcode", "description", "unitprice"]].drop_duplicates("stockcode")
        self.all_items = self.catalog.to_dict(orient="records")
        random.seed(seed)
        np.random.seed(seed)

    def _infer_category(self, item_name: str) -> str:
        for cat, keywords in self.CATEGORY_KEYWORDS.items():
            if any(k in item_name for k in keywords):
                return cat
        return "General Merchandise"

    def augment_invoice_session(self, invoice_no: str, group: pd.DataFrame) -> List[EnrichedEvent]:
        events: List[EnrichedEvent] = []
        user_id = int(group["customerid"].iloc[0])
        country = str(group["country"].iloc[0])
        purchase_time: datetime = group["invoicedate"].iloc[0]

        session_id = f"sess_{invoice_no}_{uuid.uuid4().hex[:6]}"
        device = random.choices(self.DEVICE_TYPES, weights=self.DEVICE_PROBS)[0]

        purchased_items = group.to_dict(orient="records")

        # 1. Add 1-3 abandoned browse items (items viewed but NOT bought)
        abandoned_count = random.choices([0, 1, 2, 3], weights=[0.2, 0.4, 0.3, 0.1])[0]
        abandoned_items = random.sample(self.all_items, k=min(abandoned_count, len(self.all_items)))

        # 2. Timeline construction:
        # Session begins 15 to 60 minutes before checkout
        session_duration_minutes = random.randint(15, 60)
        session_start = purchase_time - timedelta(minutes=session_duration_minutes)

        current_time = session_start

        # --- A. VIEW EVENTS (for both abandoned items and purchased items) ---
        all_browse_items = abandoned_items + purchased_items
        random.shuffle(all_browse_items)

        for item in all_browse_items:
            current_time += timedelta(seconds=random.randint(10, 90))
            item_name = str(item["description"]).strip().lower()
            events.append(
                EnrichedEvent(
                    event_id=str(uuid.uuid4()),
                    session_id=session_id,
                    user_id=user_id,
                    item_id=str(item["stockcode"]).strip(),
                    item_name=item_name,
                    category=self._infer_category(item_name),
                    price=float(item["unitprice"]),
                    quantity=1,
                    event_type="view",
                    device_type=device,
                    country=country,
                    timestamp=current_time.isoformat(),
                    event_date=current_time.strftime("%Y-%m-%d"),
                )
            )

        # --- B. ADD_TO_CART EVENTS (only for purchased items, plus 20% of abandoned items) ---
        for item in all_browse_items:
            is_purchased = any(p["stockcode"] == item["stockcode"] for p in purchased_items)
            # Purchased items always added to cart; abandoned items have 20% cart drop-off
            if is_purchased or random.random() < 0.20:
                current_time += timedelta(seconds=random.randint(15, 60))
                item_name = str(item["description"]).strip().lower()
                qty = int(item.get("quantity", 1))
                events.append(
                    EnrichedEvent(
                        event_id=str(uuid.uuid4()),
                        session_id=session_id,
                        user_id=user_id,
                        item_id=str(item["stockcode"]).strip(),
                        item_name=item_name,
                        category=self._infer_category(item_name),
                        price=float(item["unitprice"]),
                        quantity=qty,
                        event_type="add_to_cart",
                        device_type=device,
                        country=country,
                        timestamp=current_time.isoformat(),
                        event_date=current_time.strftime("%Y-%m-%d"),
                    )
                )

        # --- C. PURCHASE EVENTS (checkout confirmation for purchased items) ---
        current_time = max(current_time, purchase_time)
        for item in purchased_items:
            item_name = str(item["description"]).strip().lower()
            events.append(
                EnrichedEvent(
                    event_id=str(uuid.uuid4()),
                    session_id=session_id,
                    user_id=user_id,
                    item_id=str(item["stockcode"]).strip(),
                    item_name=item_name,
                    category=self._infer_category(item_name),
                    price=float(item["unitprice"]),
                    quantity=int(item["quantity"]),
                    event_type="purchase",
                    device_type=device,
                    country=country,
                    timestamp=current_time.isoformat(),
                    event_date=current_time.strftime("%Y-%m-%d"),
                )
            )

        return events


class KafkaStreamProducer:
    """Publishes stream events to Kafka or simulates output in dry-run mode."""

    def __init__(self, bootstrap_servers: Optional[str], topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

        if self.bootstrap_servers:
            try:
                from kafka import KafkaProducer
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8"),
                    acks="all",
                    retries=3,
                )
                logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}. Target topic: '{self.topic}'")
            except Exception as e:
                logger.warning(f"Could not connect to Kafka broker ({e}). Running in Dry-Run mode.")

    def publish_event(self, event: EnrichedEvent) -> None:
        payload = asdict(event)
        if self.producer:
            self.producer.send(self.topic, key=str(event.user_id), value=payload)
        else:
            # Dry-run logging sample
            pass

    def flush(self) -> None:
        if self.producer:
            self.producer.flush()


def run_pipeline(
    max_invoices: Optional[int] = 500,
    kafka_bootstrap: Optional[str] = None,
    topic: str = "ecom.events.raw",
    rate_limit: float = 0.0,
    export_parquet: Optional[Path] = None,
) -> None:
    # 1. Load Clean Real Kaggle Data
    loader = KaggleDataLoader(raw_dir=Path("data/raw_kaggle"), fallback_csv=Path("data/online_retail.csv"))
    df = loader.load_clean_data()

    # 2. Initialize Augmentor
    augmentor = BehaviorFunnelAugmentor(df)
    producer = KafkaStreamProducer(bootstrap_servers=kafka_bootstrap, topic=topic)

    # 3. Process Invoices
    grouped = df.groupby("invoiceno", sort=False)
    total_invoices = min(len(grouped), max_invoices) if max_invoices else len(grouped)

    logger.info(f"Processing and streaming {total_invoices:,} real customer purchase sessions...")

    all_events: List[EnrichedEvent] = []
    event_counter = 0
    start_time = time.time()

    for idx, (inv_no, grp) in enumerate(grouped):
        if max_invoices and idx >= max_invoices:
            break

        session_events = augmentor.augment_invoice_session(str(inv_no), grp)
        for ev in session_events:
            event_counter += 1
            producer.publish_event(ev)
            if export_parquet:
                all_events.append(ev)

            if rate_limit > 0:
                time.sleep(rate_limit)

        if (idx + 1) % 100 == 0 or (idx + 1) == total_invoices:
            elapsed = time.time() - start_time
            rate = event_counter / elapsed if elapsed > 0 else 0
            logger.info(f"Progress: {idx + 1:,}/{total_invoices:,} invoices -> {event_counter:,} events ({rate:.1f} events/sec)")

    producer.flush()

    # 4. Optional Parquet Export for Lakehouse
    if export_parquet and all_events:
        export_parquet.parent.mkdir(parents=True, exist_ok=True)
        pdf = pd.DataFrame([asdict(e) for e in all_events])
        pdf.to_parquet(export_parquet, index=False, engine="pyarrow")
        logger.info(f"Exported {len(pdf):,} augmented events to Parquet: {export_parquet} ({export_parquet.stat().st_size / 1024 / 1024:.2f} MB)")

    logger.info(f"Pipeline finished! Total generated & streamed events: {event_counter:,}")


def main():
    parser = argparse.ArgumentParser(description="Real Kaggle Ingestion & E-Commerce Behavior Funnel Streamer.")
    parser.add_argument("--max-invoices", type=int, default=1000, help="Max real invoices to process (default: 1,000; 0 for all)")
    parser.add_argument("--kafka-bootstrap", type=str, default=None, help="Kafka broker URI (e.g. localhost:9092)")
    parser.add_argument("--topic", type=str, default="ecom.events.raw", help="Target Kafka topic")
    parser.add_argument("--rate-limit", type=float, default=0.0, help="Sleep interval between events (seconds)")
    parser.add_argument("--export-parquet", type=str, default="data/lakehouse/augmented_stream_events.parquet",
                        help="Path to export augmented stream events as Parquet")

    args = parser.parse_args()
    limit = None if args.max_invoices <= 0 else args.max_invoices
    export_path = Path(args.export_parquet) if args.export_parquet else None

    run_pipeline(
        max_invoices=limit,
        kafka_bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        rate_limit=args.rate_limit,
        export_parquet=export_path,
    )


if __name__ == "__main__":
    main()

"""
streaming/real_data_producer.py
Real-Time Event Streamer & Kafka Producer using 100% Real Kaggle Transaction Data.

Replays actual historical e-commerce transactions as live streaming events:
- Groups items by actual customer purchase session (InvoiceNo).
- Decomposes each real transaction into a realistic shopping lifecycle:
    view -> add_to_cart -> purchase
- Streams events into Apache Kafka topic (key=CustomerID) with configurable playback speed.
- Supports dry-run simulation mode to test event formatting and throughput without a live Kafka broker.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("RealDataStreamer")


@dataclass
class EcommerceStreamEvent:
    event_id: str
    session_id: str
    user_id: int
    item_id: str
    item_name: str
    price: float
    quantity: int
    country: str
    event_type: str  # 'view', 'add_to_cart', 'purchase'
    timestamp: str
    event_date: str


class RealDataStreamReplayer:
    """Reads real Kaggle/UCI transaction data and yields a live event stream."""

    def __init__(self, data_path: Path, max_invoices: Optional[int] = None):
        self.data_path = data_path
        self.max_invoices = max_invoices

    def _load_data(self) -> pd.DataFrame:
        logger.info(f"Loading real transaction data from: {self.data_path}")
        if self.data_path.suffix == ".parquet":
            df = pd.read_parquet(self.data_path)
        else:
            try:
                df = pd.read_csv(self.data_path, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(self.data_path, encoding="ISO-8859-1")

        # Standardize column names
        df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]

        col_map = {
            "invoiceno": "invoiceno", "invoice": "invoiceno",
            "stockcode": "stockcode",
            "description": "description",
            "quantity": "quantity",
            "invoicedate": "invoicedate",
            "unitprice": "unitprice", "price": "unitprice",
            "customerid": "customerid", "customer id": "customerid",
            "country": "country"
        }
        df = df.rename(columns={c: col_map[c] for c in df.columns if c in col_map})

        # Basic cleanup
        df = df.dropna(subset=["invoiceno", "description", "customerid"])
        df = df[df["quantity"] > 0]
        df = df[df["unitprice"] > 0]
        df = df[~df["invoiceno"].astype(str).str.startswith("C")]

        df["customerid"] = df["customerid"].astype(int)
        df["invoicedate"] = pd.to_datetime(df["invoicedate"])
        df = df.sort_values("invoicedate").reset_index(drop=True)

        logger.info(f"Loaded {len(df):,} valid real transactions across {df['invoiceno'].nunique():,} unique invoices.")
        return df

    def generate_event_stream(self) -> Iterator[EcommerceStreamEvent]:
        """Generates continuous stream of events derived from real historical orders."""
        df = self._load_data()
        grouped = df.groupby("invoiceno", sort=False)

        invoice_count = 0
        for invoice_no, group in grouped:
            invoice_count += 1
            if self.max_invoices and invoice_count > self.max_invoices:
                break

            user_id = int(group["customerid"].iloc[0])
            country = str(group["country"].iloc[0])
            session_id = f"sess_{invoice_no}"
            now_dt = datetime.now(timezone.utc)
            now_iso = now_dt.isoformat()
            today_str = now_dt.strftime("%Y-%m-%d")

            items = group.to_dict(orient="records")

            # 1. Simulate browse/view events for each item in the basket
            for item in items:
                yield EcommerceStreamEvent(
                    event_id=str(uuid.uuid4()),
                    session_id=session_id,
                    user_id=user_id,
                    item_id=str(item["stockcode"]).strip(),
                    item_name=str(item["description"]).strip().lower(),
                    price=float(item["unitprice"]),
                    quantity=1,
                    country=country,
                    event_type="view",
                    timestamp=now_iso,
                    event_date=today_str,
                )

            # 2. Simulate add_to_cart events
            for item in items:
                yield EcommerceStreamEvent(
                    event_id=str(uuid.uuid4()),
                    session_id=session_id,
                    user_id=user_id,
                    item_id=str(item["stockcode"]).strip(),
                    item_name=str(item["description"]).strip().lower(),
                    price=float(item["unitprice"]),
                    quantity=int(item["quantity"]),
                    country=country,
                    event_type="add_to_cart",
                    timestamp=now_iso,
                    event_date=today_str,
                )

            # 3. Simulate final purchase confirmation for the basket
            for item in items:
                yield EcommerceStreamEvent(
                    event_id=str(uuid.uuid4()),
                    session_id=session_id,
                    user_id=user_id,
                    item_id=str(item["stockcode"]).strip(),
                    item_name=str(item["description"]).strip().lower(),
                    price=float(item["unitprice"]),
                    quantity=int(item["quantity"]),
                    country=country,
                    event_type="purchase",
                    timestamp=now_iso,
                    event_date=today_str,
                )


def run_producer(
    data_path: Path,
    kafka_bootstrap: Optional[str] = None,
    topic: str = "real_ecommerce_events",
    delay_sec: float = 0.01,
    max_invoices: Optional[int] = 1000,
) -> None:
    replayer = RealDataStreamReplayer(data_path, max_invoices=max_invoices)
    stream = replayer.generate_event_stream()

    producer = None
    if kafka_bootstrap:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
            )
            logger.info(f"Connected to Kafka broker at {kafka_bootstrap}. Streaming to topic '{topic}'...")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka ({e}). Falling back to stdout dry-run mode.")

    event_count = 0
    start_time = time.time()

    for event in stream:
        event_dict = asdict(event)
        event_count += 1

        if producer:
            producer.send(topic, key=str(event.user_id), value=event_dict)
        else:
            if event_count <= 5 or event_count % 500 == 0:
                logger.info(f"[DRY-RUN STREAM] Event #{event_count:,}: user={event.user_id} | {event.event_type.upper():<11} | {event.item_name[:30]:<30} | ${event.price:.2f}")

        if delay_sec > 0:
            time.sleep(delay_sec)

        if event_count % 1000 == 0:
            elapsed = time.time() - start_time
            throughput = event_count / elapsed if elapsed > 0 else 0
            logger.info(f"Streamed {event_count:,} events ({throughput:.1f} events/sec)...")

    if producer:
        producer.flush()
        logger.info(f"Successfully flushed {event_count:,} events to Kafka topic '{topic}'.")
    else:
        logger.info(f"Completed dry-run streaming simulation of {event_count:,} real events.")


def main():
    parser = argparse.ArgumentParser(description="Stream real Kaggle transaction data to Kafka.")
    parser.add_argument("--data-path", type=str, default="data/lakehouse/real_transactions.parquet",
                        help="Path to real data (.parquet or .csv)")
    parser.add_argument("--fallback-csv", type=str, default="data/online_retail.csv",
                        help="Fallback CSV path if parquet does not exist")
    parser.add_argument("--kafka-bootstrap", type=str, default=None,
                        help="Kafka broker address (e.g., localhost:9092). If None, runs dry-run simulation.")
    parser.add_argument("--topic", type=str, default="real_ecommerce_events", help="Kafka topic name")
    parser.add_argument("--delay", type=float, default=0.001, help="Delay between events in seconds (default: 0.001)")
    parser.add_argument("--max-invoices", type=int, default=500, help="Max real invoices to stream (default: 500)")

    args = parser.parse_args()

    data_path = Path(args.data_path)
    if not data_path.exists():
        fallback = Path(args.fallback_csv)
        if fallback.exists():
            data_path = fallback
        else:
            logger.error(f"Neither {data_path} nor {fallback} exists. Run data_loader/ingest_kaggle_data.py first.")
            sys.exit(1)

    run_producer(
        data_path=data_path,
        kafka_bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        delay_sec=args.delay,
        max_invoices=args.max_invoices,
    )


if __name__ == "__main__":
    main()

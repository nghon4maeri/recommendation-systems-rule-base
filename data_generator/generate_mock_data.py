"""
Mock Data Generator for Hybrid Recommendation System & Data Engineering Pipeline.

Generates realistic e-commerce clickstream and transaction data:
- Follows Pareto (80/20) power-law distribution for popular products.
- Injects correlated product bundles (Market Basket associations) for FP-Growth mining.
- Models diurnal and weekly shopping cycles (peak hours, weekend surges).
- Models conversion funnels: view -> add_to_cart -> purchase.
- Exports to Partitioned Parquet, CSV, or streams directly to Kafka.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Optional Faker import with robust built-in fallback
try:
    from faker import Faker
    HAS_FAKER = True
except ImportError:
    HAS_FAKER = False

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("DataGenerator")


class TextHelper:
    """Helper for generating company names and colors if Faker is not installed."""

    ADJECTIVES = ["Vintage", "Modern", "Classic", "Wireless", "Eco", "Pro", "Smart", "Ultra", "Deluxe", "Sport"]
    COLORS = ["Black", "White", "Navy", "Crimson", "Slate", "Rose Gold", "Charcoal", "Emerald", "Amber"]
    BRANDS = ["Aero", "Nexus", "Vertex", "Lumina", "Zenith", "Prime", "Echo", "Atlas", "Solas", "Krono"]

    def __init__(self, seed: int = 42):
        if HAS_FAKER:
            self.faker = Faker()
            Faker.seed(seed)
        else:
            self.faker = None

    def get_color(self) -> str:
        return self.faker.color_name().capitalize() if self.faker else random.choice(self.COLORS)

    def get_brand(self) -> str:
        return self.faker.company().split()[0] if self.faker else random.choice(self.BRANDS)

    def get_adjective(self) -> str:
        return random.choice(self.ADJECTIVES)


@dataclass(frozen=True)
class Product:
    item_id: str
    item_name: str
    category: str
    base_price: float
    bundle_id: Optional[str] = None


class CatalogGenerator:
    """Generates a realistic catalog with inherent basket association bundles."""

    CATEGORIES = {
        "Electronics": ["Laptop", "Smartphone", "Headphones", "Mechanical Keyboard", "Monitor", "Mouse"],
        "Accessories": ["Phone Case", "Screen Protector", "USB-C Hub", "Laptop Sleeve", "Wireless Charger"],
        "Home & Kitchen": ["Espresso Machine", "Coffee Beans", "Ceramic Mug", "Milk Frother", "Blender"],
        "Fitness": ["Yoga Mat", "Resistance Bands", "Foam Roller", "Water Bottle", "Smart Watch"],
        "Fashion": ["Cotton T-Shirt", "Denim Jacket", "Sneakers", "Leather Belt", "Canvas Tote Bag"],
    }

    # Intentional affinity bundles for market basket analysis (FP-Growth discovery)
    AFFINITY_BUNDLES: Dict[str, List[Tuple[str, str, float]]] = {
        "BUNDLE_TECH": [
            ("Ultrabook Pro 14", "Electronics", 1299.0),
            ("Ergonomic Wireless Mouse", "Electronics", 49.0),
            ("Multi-Port USB-C Hub", "Accessories", 39.0),
            ("Laptop Stand Aluminium", "Accessories", 35.0),
        ],
        "BUNDLE_COFFEE": [
            ("Italian Espresso Maker", "Home & Kitchen", 189.0),
            ("Premium Arabica Beans 1kg", "Home & Kitchen", 24.0),
            ("Electric Milk Frother", "Home & Kitchen", 32.0),
            ("Ceramic Coffee Mug Set", "Home & Kitchen", 28.0),
        ],
        "BUNDLE_GYM": [
            ("High-Density Yoga Mat", "Fitness", 45.0),
            ("Resistance Bands Set", "Fitness", 22.0),
            ("Deep Tissue Foam Roller", "Fitness", 29.0),
            ("Insulated Sport Flask", "Fitness", 19.0),
        ],
        "BUNDLE_MOBILE": [
            ("Flagship Smartphone 5G", "Electronics", 899.0),
            ("Shockproof Phone Case", "Accessories", 19.0),
            ("Tempered Glass Screen Protector", "Accessories", 12.0),
            ("Fast Wireless Charging Pad", "Accessories", 29.0),
        ],
    }

    def __init__(self, num_items: int = 1000, seed: int = 42):
        self.num_items = num_items
        self.text_helper = TextHelper(seed=seed)
        random.seed(seed)
        np.random.seed(seed)

    def generate(self) -> List[Product]:
        catalog: List[Product] = []
        item_counter = 1

        # 1. Add affinity bundle items first
        for bundle_id, items in self.AFFINITY_BUNDLES.items():
            for name, cat, price in items:
                catalog.append(
                    Product(
                        item_id=f"ITEM_{item_counter:05d}",
                        item_name=name,
                        category=cat,
                        base_price=price,
                        bundle_id=bundle_id,
                    )
                )
                item_counter += 1

        # 2. Fill the rest of the catalog with randomized items
        categories = list(self.CATEGORIES.keys())
        while len(catalog) < self.num_items:
            cat = random.choice(categories)
            base_noun = random.choice(self.CATEGORIES[cat])
            color = self.text_helper.get_color()
            brand = self.text_helper.get_brand()
            name = f"{brand} {color} {base_noun}"
            price = round(float(np.random.gamma(shape=3.0, scale=15.0) + 5.0), 2)

            catalog.append(
                Product(
                    item_id=f"ITEM_{item_counter:05d}",
                    item_name=name,
                    category=cat,
                    base_price=price,
                    bundle_id=None,
                )
            )
            item_counter += 1

        logger.info(f"Generated catalog with {len(catalog)} products ({len(self.AFFINITY_BUNDLES)} affinity bundles).")
        return catalog


class EventStreamSimulator:
    """Simulates realistic e-commerce user activity adhering to Pareto distribution and conversion funnels."""

    DEVICE_CHOICES = ["mobile", "desktop", "tablet"]
    DEVICE_WEIGHTS = [0.65, 0.30, 0.05]

    def __init__(
        self,
        catalog: List[Product],
        num_users: int = 5000,
        start_date: Optional[datetime] = None,
        days: int = 30,
        seed: int = 42,
    ):
        self.catalog = catalog
        self.num_users = num_users
        self.start_date = start_date or (datetime.now() - timedelta(days=days))
        self.days = days
        random.seed(seed)
        np.random.seed(seed)

        # Precompute power-law popularity weights for items (Zipf/Pareto: 80/20)
        num_items = len(self.catalog)
        ranks = np.arange(1, num_items + 1)
        zipf_weights = 1.0 / (ranks ** 1.15)
        self.item_probabilities = zipf_weights / np.sum(zipf_weights)

        # Precompute user engagement weights (Some power users interact 10x more)
        user_ranks = np.arange(1, num_users + 1)
        user_weights = 1.0 / (user_ranks ** 0.8)
        self.user_probabilities = user_weights / np.sum(user_weights)

        # Map bundle items for quick association lookup
        self.bundle_map: Dict[str, List[Product]] = {}
        for p in self.catalog:
            if p.bundle_id:
                self.bundle_map.setdefault(p.bundle_id, []).append(p)

    def _sample_timestamp(self) -> datetime:
        """Samples a timestamp reflecting diurnal peaks (lunch & evening) and weekend traffic."""
        day_offset = random.uniform(0, self.days)
        base_time = self.start_date + timedelta(days=day_offset)

        # Hour distribution: peaks at 12-14h and 20-23h
        hour_weights = [
            1, 1, 1, 1, 1, 2,     # 00-05h
            3, 4, 6, 7, 8, 9,     # 06-11h
            12, 11, 8, 8, 8, 9,   # 12-17h
            11, 13, 14, 13, 8, 4  # 18-23h
        ]
        hour_probs = np.array(hour_weights) / sum(hour_weights)
        hour = int(np.random.choice(24, p=hour_probs))
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        dt = base_time.replace(hour=hour, minute=minute, second=second)
        return dt

    def generate_events(self, target_events: int = 100000) -> pd.DataFrame:
        logger.info(f"Starting simulation to generate ~{target_events:,} events...")
        records: List[Dict] = []
        events_generated = 0

        while events_generated < target_events:
            # 1. Sample User & Session
            user_idx = int(np.random.choice(self.num_users, p=self.user_probabilities)) + 1
            session_id = str(uuid.uuid4())
            device = random.choices(self.DEVICE_CHOICES, weights=self.DEVICE_WEIGHTS)[0]
            session_start = self._sample_timestamp()
            current_time = session_start

            # 2. Sample anchor product for session based on Pareto distribution
            anchor_idx = int(np.random.choice(len(self.catalog), p=self.item_probabilities))
            anchor_product = self.catalog[anchor_idx]

            session_products = [anchor_product]

            # 3. Market Basket Cross-Sell: If anchor product belongs to a bundle, inject companions with high prob
            if anchor_product.bundle_id and anchor_product.bundle_id in self.bundle_map:
                bundle_peers = [p for p in self.bundle_map[anchor_product.bundle_id] if p.item_id != anchor_product.item_id]
                for peer in bundle_peers:
                    if random.random() < 0.75:  # 75% co-interaction probability for bundle items
                        session_products.append(peer)

            # Add 1-2 random browse items
            additional_items_cnt = random.randint(0, 2)
            if additional_items_cnt > 0:
                rand_indices = np.random.choice(len(self.catalog), size=additional_items_cnt, p=self.item_probabilities)
                session_products.extend([self.catalog[i] for i in rand_indices])

            # 4. Simulate Conversion Funnel for each item in the session
            # view (100%) -> add_to_cart (20-30%) -> purchase (40-60% of cart)
            for prod in session_products:
                current_time += timedelta(seconds=random.randint(15, 180))

                # Event: VIEW
                view_event = {
                    "event_id": str(uuid.uuid4()),
                    "session_id": session_id,
                    "user_id": user_idx,
                    "item_id": prod.item_id,
                    "item_name": prod.item_name,
                    "category": prod.category,
                    "price": prod.base_price,
                    "event_type": "view",
                    "quantity": 1,
                    "device": device,
                    "timestamp": current_time.isoformat(),
                    "event_date": current_time.strftime("%Y-%m-%d"),
                }
                records.append(view_event)
                events_generated += 1
                if events_generated >= target_events:
                    break

                # Conversion to ADD_TO_CART
                cart_prob = 0.40 if prod.bundle_id else 0.20
                if random.random() < cart_prob:
                    current_time += timedelta(seconds=random.randint(10, 60))
                    cart_event = {
                        "event_id": str(uuid.uuid4()),
                        "session_id": session_id,
                        "user_id": user_idx,
                        "item_id": prod.item_id,
                        "item_name": prod.item_name,
                        "category": prod.category,
                        "price": prod.base_price,
                        "event_type": "add_to_cart",
                        "quantity": 1,
                        "device": device,
                        "timestamp": current_time.isoformat(),
                        "event_date": current_time.strftime("%Y-%m-%d"),
                    }
                    records.append(cart_event)
                    events_generated += 1
                    if events_generated >= target_events:
                        break

                    # Conversion to PURCHASE
                    purchase_prob = 0.55 if prod.bundle_id else 0.35
                    if random.random() < purchase_prob:
                        current_time += timedelta(seconds=random.randint(30, 240))
                        purchased_qty = 1 if prod.base_price > 100 else random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
                        purchase_event = {
                            "event_id": str(uuid.uuid4()),
                            "session_id": session_id,
                            "user_id": user_idx,
                            "item_id": prod.item_id,
                            "item_name": prod.item_name,
                            "category": prod.category,
                            "price": prod.base_price,
                            "event_type": "purchase",
                            "quantity": purchased_qty,
                            "device": device,
                            "timestamp": current_time.isoformat(),
                            "event_date": current_time.strftime("%Y-%m-%d"),
                        }
                        records.append(purchase_event)
                        events_generated += 1
                        if events_generated >= target_events:
                            break

        df = pd.DataFrame(records)
        logger.info(f"Successfully generated {len(df):,} events.")
        return df


class MockDataPublisher:
    """Handles exporting data to Parquet partitions, CSV, or publishing to Kafka topic."""

    @staticmethod
    def save_to_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        target_path = output_dir / "events.parquet"
        partition_dir = output_dir / "events_partitioned"
        logger.info(f"Writing dataset to Parquet (partitioned by event_date) at: {partition_dir}")
        df.to_parquet(partition_dir, partition_cols=["event_date"], index=False, engine="pyarrow")
        # Also save unified file for fast single-table consumption
        df.to_parquet(target_path, index=False, engine="pyarrow")
        logger.info(f"Saved monolithic Parquet to {target_path} (Size: {target_path.stat().st_size / 1024 / 1024:.2f} MB)")
        return target_path

    @staticmethod
    def save_to_csv(df: pd.DataFrame, output_file: Path) -> Path:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing dataset to CSV at: {output_file}")
        df.to_csv(output_file, index=False)
        logger.info(f"Saved CSV to {output_file} (Size: {output_file.stat().st_size / 1024 / 1024:.2f} MB)")
        return output_file

    @staticmethod
    def stream_to_kafka(df: pd.DataFrame, bootstrap_servers: str, topic: str, batch_size: int = 500) -> None:
        """Publishes generated events to Kafka topic."""
        try:
            from kafka import KafkaProducer
        except ImportError:
            try:
                from confluent_kafka import Producer
            except ImportError:
                logger.error("Neither 'kafka-python' nor 'confluent-kafka' is installed. Run `pip install kafka-python`.")
                return

        logger.info(f"Connecting to Kafka at {bootstrap_servers}, publishing to topic '{topic}'...")
        try:
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
            )

            records = df.to_dict(orient="records")
            for i, record in enumerate(records):
                producer.send(topic, key=str(record["user_id"]), value=record)
                if (i + 1) % batch_size == 0:
                    producer.flush()
                    logger.info(f"Published {i + 1:,}/{len(records):,} events to Kafka topic '{topic}'...")

            producer.flush()
            logger.info("All events published to Kafka successfully.")
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}")


def print_summary_statistics(df: pd.DataFrame) -> None:
    """Displays key metrics verifying the realism of generated distributions."""
    logger.info("==================================================")
    logger.info(">> DATA SUMMARY & DISTRIBUTION VERIFICATION")
    logger.info("==================================================")
    logger.info(f"Total Events: {len(df):,}")
    logger.info(f"Unique Users: {df['user_id'].nunique():,}")
    logger.info(f"Unique Items: {df['item_id'].nunique():,}")
    logger.info(f"Date Range:   {df['event_date'].min()} to {df['event_date'].max()}")

    logger.info("\n>> Conversion Funnel Breakdown:")
    funnel = df["event_type"].value_counts(normalize=True) * 100
    for event_type, pct in funnel.items():
        logger.info(f"  - {event_type:<12}: {df['event_type'].value_counts()[event_type]:,} ({pct:.2f}%)")

    # Verify Pareto Principle
    item_purchases = df[df["event_type"] == "purchase"]["item_id"].value_counts()
    top_20_pct_items_count = int(df["item_id"].nunique() * 0.2)
    top_items_volume = item_purchases.iloc[:top_20_pct_items_count].sum()
    total_volume = item_purchases.sum()
    pareto_share = (top_items_volume / total_volume * 100) if total_volume > 0 else 0
    logger.info(f"\n>> Pareto Check: Top 20% products account for {pareto_share:.1f}% of all purchases.")
    logger.info("==================================================")


def main():
    parser = argparse.ArgumentParser(description="Generate realistic e-commerce mock dataset for DE & MLOps.")
    parser.add_argument("--num-events", type=int, default=100000, help="Number of events to generate (default: 100,000)")
    parser.add_argument("--num-users", type=int, default=5000, help="Number of unique users (default: 5,000)")
    parser.add_argument("--num-items", type=int, default=1000, help="Number of products in catalog (default: 1,000)")
    parser.add_argument("--days", type=int, default=30, help="Date range span in days (default: 30)")
    parser.add_argument("--output-dir", type=str, default="data/mock", help="Target output directory")
    parser.add_argument("--format", choices=["parquet", "csv", "both"], default="both", help="Export format")
    parser.add_argument("--kafka-bootstrap", type=str, default=None, help="Kafka bootstrap servers (e.g., localhost:9092)")
    parser.add_argument("--kafka-topic", type=str, default="ecommerce_events", help="Kafka topic name")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")

    args = parser.parse_args()

    # 1. Catalog Generation
    catalog_gen = CatalogGenerator(num_items=args.num_items, seed=args.seed)
    catalog = catalog_gen.generate()

    # 2. Event Simulation
    simulator = EventStreamSimulator(
        catalog=catalog,
        num_users=args.num_users,
        days=args.days,
        seed=args.seed,
    )
    df_events = simulator.generate_events(target_events=args.num_events)

    # 3. Print Verification Stats
    print_summary_statistics(df_events)

    # 4. Export
    out_dir = Path(args.output_dir)
    publisher = MockDataPublisher()

    if args.format in ["parquet", "both"]:
        publisher.save_to_parquet(df_events, out_dir)

    if args.format in ["csv", "both"]:
        publisher.save_to_csv(df_events, out_dir / "events.csv")

    # 5. Optional Kafka Streaming
    if args.kafka_bootstrap:
        publisher.stream_to_kafka(df_events, args.kafka_bootstrap, args.kafka_topic)


if __name__ == "__main__":
    main()

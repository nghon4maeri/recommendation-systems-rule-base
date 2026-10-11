# filepath: streaming/consumers/cart_session_consumer.py
"""
Senior Data Engineer Production Module:
Real-Time Kafka Consumer for Active Cart Sessionization & Redis Feature Store Aggregation.

Architecture:
- Consumes JSON events from Kafka topic 'ecom.events.raw'.
- Aggregates live user shopping sessions by 'session_id'.
- Stores active cart state in Redis Hash with TTL 3600s (sliding session window).
- Updates user recent browsing history in Redis List.
- Handles graceful shutdown, backpressure, and automatic reconnections.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional

import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("CartSessionConsumer")


class RedisStoreManager:
    """Manages Redis connection and low-latency atomic feature store operations."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: Optional[str] = None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.client: Optional[redis.Redis] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
            self.client.ping()
            logger.info(f"Connected successfully to Redis at {self.host}:{self.port}/{self.db}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis at {self.host}:{self.port}: {e}")
            raise

    def add_to_cart(self, session_id: str, user_id: int, item_id: str, item_name: str, price: float, quantity: int, ttl: int = 3600) -> None:
        if not self.client:
            return

        cart_key = f"cart:session:{session_id}"
        user_sess_key = f"user:session:{user_id}"

        item_data = json.dumps({
            "item_name": item_name,
            "price": price,
            "quantity": quantity,
            "updated_at": time.time(),
        })

        pipe = self.client.pipeline()
        # 1. Update session cart hash (field=item_id, value=item_data)
        pipe.hset(cart_key, item_id, item_data)
        pipe.expire(cart_key, ttl)

        # 2. Map user to active session
        pipe.set(user_sess_key, session_id, ex=ttl)

        # 3. Add to set of active sessions
        pipe.sadd("active:sessions", session_id)
        pipe.execute()

        logger.debug(f"Added item '{item_name}' (ID: {item_id}) to cart: {cart_key}")

    def complete_purchase(self, session_id: str, user_id: int, items: List[Dict[str, Any]]) -> None:
        if not self.client:
            return

        cart_key = f"cart:session:{session_id}"
        history_key = f"user:purchase_history:{user_id}"

        pipe = self.client.pipeline()
        # 1. Archive purchase history for the user (keep last 50 purchased items)
        for it in items:
            pipe.lpush(history_key, json.dumps({
                "item_id": it.get("item_id"),
                "item_name": it.get("item_name"),
                "price": it.get("price"),
                "purchased_at": time.time(),
            }))
        pipe.ltrim(history_key, 0, 49)

        # 2. Clear active cart after successful purchase
        pipe.delete(cart_key)
        pipe.srem("active:sessions", session_id)
        pipe.execute()

        logger.info(f"Session {session_id} checked out. Cleared active cart and updated history for User {user_id}.")

    def record_view(self, user_id: int, item_id: str, item_name: str) -> None:
        if not self.client:
            return

        views_key = f"user:recent_views:{user_id}"
        payload = json.dumps({"item_id": item_id, "item_name": item_name, "viewed_at": time.time()})

        pipe = self.client.pipeline()
        pipe.lpush(views_key, payload)
        pipe.ltrim(views_key, 0, 19)  # Keep last 20 views
        pipe.expire(views_key, 86400 * 7)  # 7 days TTL
        pipe.execute()

    def get_cart_items(self, session_id: str) -> Dict[str, Dict[str, Any]]:
        if not self.client:
            return {}
        cart_key = f"cart:session:{session_id}"
        raw = self.client.hgetall(cart_key)
        parsed: Dict[str, Dict[str, Any]] = {}
        for k, v in raw.items():
            try:
                parsed[k] = json.loads(v)
            except json.JSONDecodeError:
                continue
        return parsed


class CartSessionConsumer:
    """Kafka Consumer service reading events and maintaining active session states."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        redis_manager: RedisStoreManager,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.redis_manager = redis_manager
        self.is_running = True
        self.consumer: Optional[KafkaConsumer] = None

        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, signum: int, frame: Any) -> None:
        logger.info(f"Received shutdown signal ({signum}). Gracefully closing consumer...")
        self.is_running = False

    def _init_consumer(self) -> None:
        logger.info(f"Connecting to Kafka broker(s): {self.bootstrap_servers}, group: {self.group_id}")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        logger.info(f"Successfully subscribed to Kafka topic: '{self.topic}'")

    def process_event(self, event: Dict[str, Any]) -> None:
        event_type = event.get("event_type")
        session_id = event.get("session_id")
        user_id = event.get("user_id")
        item_id = event.get("item_id")
        item_name = event.get("item_name", "")
        price = float(event.get("price", 0.0))
        quantity = int(event.get("quantity", 1))

        if not session_id or not user_id:
            logger.warning(f"Skipping malformed event lacking session_id or user_id: {event}")
            return

        if event_type == "add_to_cart":
            self.redis_manager.add_to_cart(
                session_id=session_id,
                user_id=int(user_id),
                item_id=str(item_id),
                item_name=item_name,
                price=price,
                quantity=quantity,
                ttl=3600,
            )
        elif event_type == "purchase":
            self.redis_manager.complete_purchase(
                session_id=session_id,
                user_id=int(user_id),
                items=[{"item_id": item_id, "item_name": item_name, "price": price, "quantity": quantity}],
            )
        elif event_type == "view":
            self.redis_manager.record_view(
                user_id=int(user_id),
                item_id=str(item_id),
                item_name=item_name,
            )

    def run(self) -> None:
        self._init_consumer()
        if not self.consumer:
            logger.error("Consumer not initialized. Aborting.")
            return

        logger.info("CartSessionConsumer loop started. Awaiting events...")
        event_count = 0
        start_time = time.time()

        while self.is_running:
            try:
                records = self.consumer.poll(timeout_ms=1000)
                if not records:
                    continue

                for topic_partition, messages in records.items():
                    for msg in messages:
                        payload = msg.value
                        if isinstance(payload, dict):
                            self.process_event(payload)
                            event_count += 1

                if event_count > 0 and event_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    throughput = event_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Processed {event_count:,} events ({throughput:.1f} events/sec).")

            except KafkaError as ke:
                logger.error(f"Kafka error encountered: {ke}", exc_info=True)
                time.sleep(2)
            except Exception as e:
                logger.error(f"Unexpected processing error: {e}", exc_info=True)

        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer safely closed.")


def main():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "ecom.events.raw")
    consumer_group = os.getenv("KAFKA_GROUP_ID", "recsys-cart-session-consumer")

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_pass = os.getenv("REDIS_PASSWORD", None)

    logger.info("Initializing Real-Time Cart Session Consumer Stack...")
    redis_mgr = RedisStoreManager(host=redis_host, port=redis_port, db=redis_db, password=redis_pass)
    consumer = CartSessionConsumer(
        bootstrap_servers=kafka_servers,
        topic=kafka_topic,
        group_id=consumer_group,
        redis_manager=redis_mgr,
    )
    consumer.run()


if __name__ == "__main__":
    main()

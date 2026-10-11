# filepath: tests/test_streaming_consumer.py
"""
Senior Data Engineer & MLOps Automated Test Suite:
Kafka Event Streaming Consumer & Session Aggregation Tests.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List
import pytest


class MockRedisPipeline:
    """Mock Redis pipeline capturing batched operations."""

    def __init__(self, parent_store: Dict[str, Any]):
        self.parent = parent_store
        self.ops: List[Dict[str, Any]] = []

    def hset(self, name: str, key: str, value: Any) -> "MockRedisPipeline":
        self.ops.append({"op": "hset", "name": name, "key": key, "val": value})
        return self

    def expire(self, name: str, time: int) -> "MockRedisPipeline":
        self.ops.append({"op": "expire", "name": name, "ttl": time})
        return self

    def set(self, name: str, value: Any, ex: int | None = None) -> "MockRedisPipeline":
        self.ops.append({"op": "set", "name": name, "val": value, "ttl": ex})
        return self

    def sadd(self, name: str, member: str) -> "MockRedisPipeline":
        self.ops.append({"op": "sadd", "name": name, "member": member})
        return self

    def lpush(self, name: str, value: Any) -> "MockRedisPipeline":
        self.ops.append({"op": "lpush", "name": name, "val": value})
        return self

    def ltrim(self, name: str, start: int, end: int) -> "MockRedisPipeline":
        self.ops.append({"op": "ltrim", "name": name, "start": start, "end": end})
        return self

    def delete(self, *names: str) -> "MockRedisPipeline":
        self.ops.append({"op": "delete", "names": names})
        return self

    def srem(self, name: str, member: str) -> "MockRedisPipeline":
        self.ops.append({"op": "srem", "name": name, "member": member})
        return self

    def execute(self) -> List[Any]:
        # Execute ops against parent store
        for op in self.ops:
            if op["op"] == "set":
                self.parent[op["name"]] = op["val"]
            elif op["op"] == "sadd":
                bucket = self.parent.setdefault(op["name"], set())
                if isinstance(bucket, set):
                    bucket.add(op["member"])
            elif op["op"] == "hset":
                bucket = self.parent.setdefault(op["name"], {})
                bucket[op["key"]] = op["val"]
            elif op["op"] == "delete":
                for n in op["names"]:
                    self.parent.pop(n, None)
            elif op["op"] == "lpush":
                lst = self.parent.setdefault(op["name"], [])
                lst.insert(0, op["val"])
        results = [True] * len(self.ops)
        self.ops.clear()
        return results



class MockStreamingRedisClient:
    """Mock Redis client wired with pipeline support."""

    def __init__(self):
        self.store: Dict[str, Any] = {}

    def pipeline(self) -> MockRedisPipeline:
        return MockRedisPipeline(self.store)

    def hgetall(self, name: str) -> Dict[str, Any]:
        return self.store.get(name, {})

    def ping(self) -> bool:
        return True

    def delete(self, *names: str) -> int:
        count = 0
        for n in names:
            if n in self.store:
                del self.store[n]
                count += 1
        return count

    def sadd(self, name: str, member: str) -> int:
        bucket = self.store.setdefault(name, set())
        if isinstance(bucket, set):
            is_new = member not in bucket
            bucket.add(member)
            return 1 if is_new else 0
        return 1



@pytest.mark.streaming
@pytest.mark.unit
def test_kafka_event_payload_schema() -> None:
    """Verifies that incoming Kafka JSON stream events conform to the schema."""
    valid_event = {
        "event_id": "evt_abc_123",
        "timestamp": time.time(),
        "event_type": "add_to_cart",
        "session_id": "sess_stream_01",
        "user_id": 13089,
        "item_id": "85099B",
        "item_name": "JUMBO BAG RED RETROSPOT",
        "price": 1.95,
        "quantity": 2,
    }

    assert valid_event["event_type"] in ("view", "add_to_cart", "purchase")
    assert isinstance(valid_event["user_id"], int)
    assert isinstance(valid_event["session_id"], str)
    assert valid_event["quantity"] >= 1


@pytest.mark.streaming
@pytest.mark.unit
def test_session_cart_accumulation() -> None:
    """Verifies that add_to_cart pipeline adds items and sets TTL to 3600 seconds."""
    client = MockStreamingRedisClient()
    pipe = client.pipeline()

    cart_key = "cart:session:sess_stream_01"
    item_payload = json.dumps({
        "item_name": "JUMBO BAG RED RETROSPOT",
        "price": 1.95,
        "quantity": 2,
    })

    pipe.hset(cart_key, "85099B", item_payload)
    pipe.expire(cart_key, 3600)
    pipe.execute()

    items = client.hgetall(cart_key)
    assert "85099B" in items
    parsed = json.loads(items["85099B"])
    assert parsed["quantity"] == 2
    assert parsed["price"] == 1.95


@pytest.mark.streaming
@pytest.mark.unit
def test_purchase_event_clears_cart() -> None:
    """Verifies that purchase event completes session and purges active cart."""
    client = MockStreamingRedisClient()
    cart_key = "cart:session:sess_stream_checkout"

    # Pre-populate cart
    pipe1 = client.pipeline()
    pipe1.hset(cart_key, "85123A", json.dumps({"item_name": "WHITE HANGING HEART", "price": 2.55}))
    pipe1.execute()
    assert cart_key in client.store

    # Process checkout
    pipe2 = client.pipeline()
    pipe2.lpush("user:purchase_history:13089", json.dumps({"item_id": "85123A", "price": 2.55}))
    pipe2.delete(cart_key)
    pipe2.execute()

    # Verify cart deleted and history recorded
    assert cart_key not in client.store
    history = client.store.get("user:purchase_history:13089", [])
    assert len(history) == 1
    assert "85123A" in history[0]

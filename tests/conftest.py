# filepath: tests/conftest.py
"""
Pytest Fixtures and Mock Environment for Hybrid Recommender Platform.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Generator, List, Optional
import pytest
from fastapi.testclient import TestClient

from backend.app.main import app
import backend.app.main as main_module
from backend.app.services.recommender import HybridRecommenderService


class MockRedis:
    """In-memory Redis mock supporting key-value, hash, and JSON serialization."""

    def __init__(self):
        self.store: Dict[str, bytes] = {}
        self.hashes: Dict[str, Dict[str, bytes]] = {}
        self.ttls: Dict[str, int] = {}

    def ping(self) -> bool:
        return True

    def get(self, name: str) -> Optional[bytes]:
        return self.store.get(name)

    def set(self, name: str, value: Any, ex: Optional[int] = None) -> bool:
        if isinstance(value, str):
            self.store[name] = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray)):
            self.store[name] = bytes(value)
        else:
            self.store[name] = str(value).encode("utf-8")
        if ex:
            self.ttls[name] = ex
        return True

    def setex(self, name: str, time: int, value: Any) -> bool:
        return self.set(name, value, ex=time)

    def expire(self, name: str, time: int) -> bool:
        self.ttls[name] = time
        return True

    def hset(self, name: str, key: str, value: Any) -> int:
        bucket = self.hashes.setdefault(name, {})
        val_bytes = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        is_new = key not in bucket
        bucket[key] = val_bytes
        return 1 if is_new else 0

    def hget(self, name: str, key: str) -> Optional[bytes]:
        return self.hashes.get(name, {}).get(key)

    def hgetall(self, name: str) -> Dict[str, bytes]:
        return self.hashes.get(name, {})

    def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        return [self.store.get(k) for k in keys]

    def delete(self, *names: str) -> int:
        count = 0
        for n in names:
            if n in self.store:
                del self.store[n]
                count += 1
            if n in self.hashes:
                del self.hashes[n]
                count += 1
        return count

    def flushdb(self) -> bool:
        self.store.clear()
        self.hashes.clear()
        self.ttls.clear()
        return True



@pytest.fixture
def mock_redis() -> MockRedis:
    """Provides a clean in-memory mock Redis client pre-populated with association rules."""
    redis = MockRedis()
    
    # Pre-populate sample FP-Growth association rules
    rules_85099B = [
        {"consequent": "85099F", "confidence": 0.68, "lift": 5.42},
        {"consequent": "20725", "confidence": 0.54, "lift": 4.15},
    ]
    rules_20725 = [
        {"consequent": "20727", "confidence": 0.75, "lift": 7.34},
        {"consequent": "20728", "confidence": 0.69, "lift": 6.81},
    ]
    
    redis.set("rules:85099b", json.dumps(rules_85099B))
    redis.set("rules:20725", json.dumps(rules_20725))
    
    # Pre-populate an active session cart
    sample_cart = {
        "session_id": "test_sess_001",
        "items": [
            {"stock_code": "85099B", "quantity": 2, "updated_at": 1700000000.0}
        ],
        "last_updated": 1700000000.0
    }
    redis.setex("cart:test_sess_001", 3600, json.dumps(sample_cart))
    
    return redis


@pytest.fixture
def recommender_service(mock_redis: MockRedis) -> HybridRecommenderService:
    """Instantiates a HybridRecommenderService wired with mock Redis."""
    service = HybridRecommenderService(redis_client=mock_redis)  # type: ignore
    return service


@pytest.fixture
def client(mock_redis: MockRedis, recommender_service: HybridRecommenderService) -> Generator[TestClient, None, None]:
    """TestClient wired with mock Redis and initialized RecommenderService."""
    main_module.redis_client = mock_redis  # type: ignore
    main_module.recommender_service = recommender_service

    with TestClient(app) as test_client:
        yield test_client

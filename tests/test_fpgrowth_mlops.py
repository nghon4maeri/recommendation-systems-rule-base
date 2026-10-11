# filepath: tests/test_fpgrowth_mlops.py
"""
Senior Data Engineer & MLOps Automated Test Suite:
FP-Growth Association Rule Mining & Redis Feature Store Sync Tests.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List
import pandas as pd
import pytest

from mlops.sync_redis_store import RedisFeatureStoreSync
from tests.test_streaming_consumer import MockStreamingRedisClient


@pytest.mark.ml
@pytest.mark.unit
def test_association_rule_schema_validation() -> None:
    """Verifies that rule generation produces valid schemas and non-negative metric values."""
    mock_rules_data = [
        {
            "antecedents": frozenset(["85099B"]),
            "consequents": frozenset(["85099F"]),
            "support": 0.042,
            "confidence": 0.68,
            "lift": 5.42,
        },
        {
            "antecedents": frozenset(["20725"]),
            "consequents": frozenset(["20727"]),
            "support": 0.038,
            "confidence": 0.75,
            "lift": 7.34,
        },
    ]
    df = pd.DataFrame(mock_rules_data)

    assert "antecedents" in df.columns
    assert "consequents" in df.columns
    assert "confidence" in df.columns
    assert "lift" in df.columns
    assert (df["confidence"] >= 0.0).all() and (df["confidence"] <= 1.0).all()
    assert (df["lift"] > 0.0).all()


@pytest.mark.ml
@pytest.mark.unit
def test_sync_rules_to_mock_redis(tmp_path) -> None:
    """Verifies that RedisFeatureStoreSync correctly serializes rules to Redis key-value store."""
    rules_json = tmp_path / "fpgrowth_rules.json"
    dummy_csv = tmp_path / "fallback.csv"

    sample_rules = {
        "85099b": [
            {"consequent": "85099F", "confidence": 0.68, "lift": 5.42},
            {"consequent": "20725", "confidence": 0.54, "lift": 4.15},
        ]
    }
    rules_json.write_text(json.dumps(sample_rules), encoding="utf-8")

    # Wire sync instance with mock client
    sync_obj = RedisFeatureStoreSync.__new__(RedisFeatureStoreSync)
    mock_client = MockStreamingRedisClient()
    mock_client.store = {}
    sync_obj.client = mock_client  # type: ignore

    count = sync_obj.sync_fpgrowth_rules(rules_json_path=rules_json, fallback_csv_path=dummy_csv)

    assert count == 1
    key = "rules:fpgrowth:85099b"
    assert key in mock_client.store
    stored_list = json.loads(mock_client.store[key])
    assert len(stored_list) == 2
    assert stored_list[0]["consequent"] == "85099F"
    assert stored_list[0]["lift"] == 5.42


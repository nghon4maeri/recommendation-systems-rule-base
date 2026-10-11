# filepath: tests/test_api.py
"""
Senior Data Engineer & MLOps Automated Test Suite:
FastAPI Serving Gateway API & Redis Integration Tests.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
import backend.app.main as main_module


@pytest.mark.api
@pytest.mark.unit
def test_health_check(client: TestClient) -> None:
    """Verifies that the /health endpoint responds 200 with service status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ("healthy", "degraded")
    assert "redis_connected" in data
    assert "ncf_model_loaded" in data
    assert "timestamp" in data


@pytest.mark.api
@pytest.mark.unit
def test_cart_add_item(client: TestClient) -> None:
    """Verifies adding an item to the session cart."""
    payload = {
        "session_id": "test_sess_cart_add",
        "user_id": 13089,
        "item_id": "85099b",
        "item_name": "JUMBO BAG RED RETROSPOT",
        "price": 1.95,
        "quantity": 1,
    }
    response = client.post("/cart/add", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "Added" in data["message"]


@pytest.mark.api
@pytest.mark.unit
def test_get_cart_existing_session(client: TestClient) -> None:
    """Verifies fetching an active session cart from Redis / in-memory store."""
    # Seed an item first
    add_payload = {
        "session_id": "test_sess_001",
        "user_id": 13089,
        "item_id": "85099b",
        "item_name": "JUMBO BAG RED RETROSPOT",
        "price": 1.95,
        "quantity": 2,
    }
    client.post("/cart/add", json=add_payload)

    response = client.get("/cart/test_sess_001")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "test_sess_001"
    assert data["total_items"] >= 1
    assert "85099b" in data["items"]


@pytest.mark.api
@pytest.mark.unit
def test_get_cart_non_existent_session(client: TestClient) -> None:
    """Verifies fetching a non-existent session cart returns 200 with empty items map."""
    response = client.get("/cart/non_existent_session_999")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "non_existent_session_999"
    assert data["items"] == {}


@pytest.mark.api
@pytest.mark.unit
def test_recommend_cart_fpgrowth(client: TestClient) -> None:
    """Verifies cart-based FP-Growth recommendations using seeded active cart."""
    # Seed cart item
    client.post(
        "/cart/add",
        json={
            "session_id": "test_sess_rec_cart",
            "user_id": 13089,
            "item_id": "85099b",
            "item_name": "jumbo bag red retrospot",
            "price": 1.95,
            "quantity": 1,
        },
    )

    response = client.get("/recommend/cart/test_sess_rec_cart?top_k=3")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "test_sess_rec_cart"
    assert "recommendations" in data
    assert isinstance(data["recommendations"], list)


@pytest.mark.api
@pytest.mark.unit
def test_recommend_user_personalized(client: TestClient) -> None:
    """Verifies user-based personalized recommendations endpoint."""
    response = client.get("/recommend/user/13089?top_k=5")
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == 13089
    assert "recommendations" in data
    assert isinstance(data["recommendations"], list)


@pytest.mark.api
@pytest.mark.unit
def test_recommend_hybrid_ensemble_post(client: TestClient) -> None:
    """Verifies dual-signal hybrid ranking endpoint via POST."""
    payload = {
        "user_id": 13089,
        "session_id": "test_sess_rec_cart",
        "top_k": 5,
        "alpha": 0.6,
    }
    response = client.post("/recommend/hybrid", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == 13089
    assert data["session_id"] == "test_sess_rec_cart"
    assert "latency_ms" in data
    assert "recommendations" in data
    assert isinstance(data["recommendations"], list)


@pytest.mark.api
@pytest.mark.unit
def test_recommend_hybrid_ensemble_get(client: TestClient) -> None:
    """Verifies dual-signal hybrid ranking endpoint via GET Query parameters."""
    response = client.get("/recommend/hybrid?user_id=13089&session_id=test_sess_rec_cart&top_k=5")
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == 13089
    assert "recommendations" in data


@pytest.mark.api
@pytest.mark.unit
def test_process_time_header_middleware(client: TestClient) -> None:
    """Verifies the x-response-time-ms custom latency middleware header is returned."""
    response = client.get("/health")
    assert "x-response-time-ms" in response.headers
    latency_ms = float(response.headers["x-response-time-ms"])
    assert latency_ms >= 0.0

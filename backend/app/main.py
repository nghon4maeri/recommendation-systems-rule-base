# filepath: backend/app/main.py
"""
Senior Data Engineer & MLOps Production Module:
FastAPI Real-Time Hybrid Recommendation Serving Gateway.

Features:
- Sub-5ms serving via Redis Feature Store lookups.
- Endpoints for Cart-based (FP-Growth), User-based (NCF), and Hybrid Ensemble recommendations.
- Latency monitoring middleware.
- Graceful connection pooling for Redis and PyTorch models.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import redis
except ImportError:
    redis = None
from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.app.services.recommender import HybridRecommenderService

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FastAPIGateway")

# Global dependencies
redis_client: Optional[redis.Redis] = None
recommender_service: Optional[HybridRecommenderService] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, recommender_service

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_pass = os.getenv("REDIS_PASSWORD", None)

    logger.info(f"Connecting to Redis at {redis_host}:{redis_port}/{redis_db}...")
    try:
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_pass,
            decode_responses=True,
            socket_timeout=5,
        )
        redis_client.ping()
        logger.info("Redis connection established.")
    except Exception as e:
        logger.warning(f"Could not connect to Redis: {e}. Running in standalone mode.")
        redis_client = None

    model_path = Path("models/ncf_model.pt")
    recommender_service = HybridRecommenderService(
        redis_client=redis_client,
        ncf_model_path=model_path,
    )
    logger.info("Hybrid Recommender Service initialized.")

    yield

    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed.")


app = FastAPI(
    title="Real-Time Streaming Recommender API",
    version="2.0.0",
    description="High-throughput, sub-5ms hybrid recommendation system using Kafka, Redis, FP-Growth, and NCF.",
    lifespan=lifespan,
)

# Enable CORS for Streamlit UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def measure_request_latency(request: Request, call_next):
    start = time.perf_counter()
    response: Response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"
    logger.info(f"{request.method} {request.url.path} | Status: {response.status_code} | Latency: {duration_ms:.2f} ms")
    return response


# --- Pydantic Schemas ---

class AddToCartRequest(BaseModel):
    session_id: str
    user_id: int
    item_id: str
    item_name: str
    price: float = 0.0
    quantity: int = 1


class RecommendationItem(BaseModel):
    item_name: str
    score: float
    type: Optional[str] = "recommendation"
    lift: Optional[float] = None
    triggered_by: Optional[str] = None


class CartRecommendResponse(BaseModel):
    session_id: str
    cart_items_count: int
    recommendations: List[RecommendationItem]


class UserRecommendResponse(BaseModel):
    user_id: int
    recommendations: List[RecommendationItem]


class HybridRequest(BaseModel):
    user_id: int
    session_id: Optional[str] = None
    top_k: int = Field(default=5, ge=1, le=20)
    alpha: float = Field(default=0.6, ge=0.0, le=1.0)


class HybridRecommendResponse(BaseModel):
    user_id: int
    session_id: Optional[str]
    latency_ms: float
    recommendations: List[Dict[str, Any]]
    debug: Dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    redis_connected: bool
    ncf_model_loaded: bool
    timestamp: float


# --- API Routes ---

@app.get("/health", response_model=HealthResponse)
def health_check():
    global redis_client, recommender_service
    redis_alive = False
    if redis_client:
        try:
            redis_alive = bool(redis_client.ping())
        except Exception:
            redis_alive = False

    ncf_alive = bool(recommender_service and recommender_service.ncf_model is not None)

    return HealthResponse(
        status="healthy" if (redis_alive or ncf_alive) else "degraded",
        redis_connected=redis_alive,
        ncf_model_loaded=ncf_alive,
        timestamp=time.time(),
    )


@app.post("/cart/add")
def add_item_to_cart(payload: AddToCartRequest):
    """Adds an item to the user's active cart in Redis or local fallback store."""
    if not recommender_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")
    recommender_service.add_to_cart(
        session_id=payload.session_id,
        user_id=payload.user_id,
        item_id=payload.item_id,
        item_name=payload.item_name,
        price=payload.price,
        quantity=payload.quantity,
    )
    return {"status": "ok", "message": f"Added '{payload.item_name}' to cart."}


@app.get("/cart/{session_id}")
def get_session_cart(session_id: str):
    """Inspects items currently stored in active Redis cart session."""
    if not recommender_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")
    items = recommender_service.get_cart_items(session_id)
    return {
        "session_id": session_id,
        "total_items": len(items),
        "items": items,
    }


@app.get("/recommend/cart/{session_id}", response_model=CartRecommendResponse)
def recommend_for_cart(session_id: str, top_k: int = Query(default=5, ge=1, le=20)):
    """
    Sub-5ms Real-Time Cross-Sell Recommendation.
    Reads active items in session cart from Redis, applies FP-Growth rules.
    """
    if not recommender_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

    cart = recommender_service.get_cart_items(session_id)
    recs = recommender_service.recommend_for_cart(session_id, top_k=top_k)

    return CartRecommendResponse(
        session_id=session_id,
        cart_items_count=len(cart),
        recommendations=recs,
    )


@app.get("/recommend/user/{user_id}", response_model=UserRecommendResponse)
def recommend_for_user(user_id: int, top_k: int = Query(default=5, ge=1, le=20)):
    """
    Personalized recommendations for a specific user ID based on NCF Deep Learning.
    Served from Redis feature cache with on-the-fly PyTorch fallback.
    """
    if not recommender_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

    recs = recommender_service.recommend_for_user(user_id=user_id, top_k=top_k)
    return UserRecommendResponse(
        user_id=user_id,
        recommendations=recs,
    )


@app.get("/recommend/item")
def recommend_by_item(item: str = Query(..., description="Item description name"), top_k: int = Query(default=5, ge=1, le=20)):
    """Direct FP-Growth lookup for frequent companion products."""
    if not recommender_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

    recs = recommender_service.recommend_for_item(item_name=item, top_k=top_k)
    return {
        "input_item": item,
        "recommendations": recs,
    }


@app.post("/recommend/hybrid", response_model=HybridRecommendResponse)
def recommend_hybrid(payload: HybridRequest):
    """
    Ensemble hybrid recommendation combining short-term cart signals with long-term user taste profile.
    """
    if not recommender_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

    result = recommender_service.recommend_hybrid(
        user_id=payload.user_id,
        session_id=payload.session_id,
        top_k=payload.top_k,
        alpha=payload.alpha,
    )
    return HybridRecommendResponse(**result)

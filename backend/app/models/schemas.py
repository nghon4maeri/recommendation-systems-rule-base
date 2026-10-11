# filepath: backend/app/models/schemas.py
"""
Pydantic Schemas for API Request and Response Payloads.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


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

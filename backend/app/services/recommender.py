# filepath: backend/app/services/recommender.py
"""
Senior Data Engineer Production Module:
Hybrid Real-Time Recommender Engine.

Features:
- Sub-5ms Redis Feature Store lookups for FP-Growth co-purchase rules and active cart states.
- On-demand / Cached Neural Collaborative Filtering (NCF) user scoring.
- Hybrid ensemble combining short-term cart intent (FP-Growth) and long-term user profile (NCF).
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import redis
except ImportError:
    redis = None
import torch

logger = logging.getLogger("HybridRecommenderService")


class HybridRecommenderService:
    """Core recommendation business logic interacting with Redis Feature Store & PyTorch NCF."""

    def __init__(
        self,
        redis_client: Optional[redis.Redis],
        ncf_model_path: Optional[Path] = None,
    ):
        self.redis = redis_client
        self.redis = redis_client
        self.ncf_model_path = ncf_model_path
        self.ncf_model: Optional[torch.nn.Module] = None
        self.user2idx: Dict[int, int] = {}
        self.item2idx: Dict[str, int] = {}
        self.idx2item: Dict[int, str] = {}
        self.local_rules: Dict[str, List[Dict[str, Any]]] = {}
        self.local_carts: Dict[str, Dict[str, Any]] = {}
        self._load_ncf_model()
        self._load_local_rules_fallback()

    def _load_local_rules_fallback(self) -> None:
        """Loads rules into local memory dictionary as fallback if Redis is offline."""
        rules_json = Path("data/models/fpgrowth_rules.json")
        rules_csv = Path("data/rules.csv")

        if rules_json.exists():
            try:
                with open(rules_json, "r", encoding="utf-8") as f:
                    self.local_rules = json.load(f)
                logger.info(f"Loaded {len(self.local_rules):,} fallback rules from JSON.")
            except Exception as e:
                logger.warning(f"Failed to read local rules JSON: {e}")
        elif rules_csv.exists():
            try:
                import pandas as pd
                df = pd.read_csv(rules_csv)
                for _, row in df.iterrows():
                    ante = str(row["antecedent"]).strip().lower()
                    cons = str(row["consequent"]).strip().lower()
                    conf = float(row.get("confidence", 0.0))
                    self.local_rules.setdefault(ante, []).append({"consequent": cons, "confidence": conf})
                logger.info(f"Loaded {len(self.local_rules):,} fallback rules from CSV.")
            except Exception as e:
                logger.warning(f"Failed to read local rules CSV: {e}")

    def _load_ncf_model(self) -> None:
        if not self.ncf_model_path or not self.ncf_model_path.exists():
            logger.warning(f"NCF model path {self.ncf_model_path} does not exist. Running in rules-only mode.")
            return

        try:
            ckpt = torch.load(self.ncf_model_path, map_location="cpu", weights_only=False)

            for k, v in ckpt.get("user2idx", {}).items():
                try:
                    self.user2idx[int(k)] = int(v)
                except ValueError:
                    continue

            self.item2idx = {str(k): int(v) for k, v in ckpt.get("item2idx", {}).items()}
            self.idx2item = {v: k for k, v in self.item2idx.items()}

            class NCF(torch.nn.Module):
                def __init__(self, n_users: int, n_items: int, emb_size: int = 64):
                    super().__init__()
                    self.u_emb = torch.nn.Embedding(n_users, emb_size)
                    self.i_emb = torch.nn.Embedding(n_items, emb_size)
                    self.fc = torch.nn.Sequential(
                        torch.nn.Linear(emb_size * 2, 128),
                        torch.nn.ReLU(),
                        torch.nn.Linear(128, 1),
                        torch.nn.Sigmoid(),
                    )

                def forward(self, u, i):
                    x = torch.cat([self.u_emb(u), self.i_emb(i)], dim=1)
                    return self.fc(x).squeeze()

            n_users = max(self.user2idx.values()) + 1
            n_items = max(self.item2idx.values()) + 1
            self.ncf_model = NCF(n_users, n_items).eval()
            self.ncf_model.load_state_dict(ckpt["model"])
            logger.info(f"Loaded NCF model for serving: {n_users:,} users, {n_items:,} items.")
        except Exception as e:
            logger.error(f"Failed to load NCF checkpoint: {e}", exc_info=True)

    def add_to_cart(self, session_id: str, user_id: int, item_id: str, item_name: str, price: float = 0.0, quantity: int = 1) -> None:
        """Adds item to active cart in Redis, or local in-memory store if Redis is unavailable."""
        if self.redis:
            cart_key = f"cart:session:{session_id}"
            item_data = json.dumps({
                "item_name": item_name,
                "price": price,
                "quantity": quantity,
                "updated_at": time.time(),
            })
            self.redis.hset(cart_key, item_id, item_data)
            self.redis.expire(cart_key, 3600)
        else:
            cart = self.local_carts.setdefault(session_id, {})
            cart[item_id] = {
                "item_name": item_name,
                "price": price,
                "quantity": quantity,
                "updated_at": time.time(),
            }

    def get_cart_items(self, session_id: str) -> Dict[str, Dict[str, Any]]:
        """Retrieves active cart contents from Redis or local in-memory store."""
        if self.redis:
            cart_key = f"cart:session:{session_id}"
            raw_items = self.redis.hgetall(cart_key)
            parsed: Dict[str, Dict[str, Any]] = {}
            for item_id, item_json in raw_items.items():
                try:
                    parsed[item_id] = json.loads(item_json)
                except json.JSONDecodeError:
                    continue
            return parsed
        return self.local_carts.get(session_id, {})

    def recommend_for_cart(self, session_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieves active cart items and computes real-time cross-sell suggestions
        from precomputed FP-Growth association rules in Redis (with local fallback).
        """
        cart_items = self.get_cart_items(session_id)
        if not cart_items:
            logger.debug(f"Cart is empty for session {session_id}.")
            return []

        cart_item_names: Set[str] = {v.get("item_name", "").strip().lower() for v in cart_items.values()}
        candidate_scores: Dict[str, float] = {}
        candidate_meta: Dict[str, Dict[str, Any]] = {}

        if self.redis:
            pipe = self.redis.pipeline()
            item_list = list(cart_item_names)
            for item_name in item_list:
                pipe.get(f"rules:fpgrowth:{item_name}")

            rule_results = pipe.execute()
            for idx, raw_rules in enumerate(rule_results):
                if not raw_rules:
                    continue
                try:
                    rules = json.loads(raw_rules)
                    source_item = item_list[idx]
                    for r in rules:
                        cons = r.get("consequent", "").strip().lower()
                        if not cons or cons in cart_item_names:
                            continue
                        conf = float(r.get("confidence", 0.0))
                        candidate_scores[cons] = candidate_scores.get(cons, 0.0) + conf
                        if cons not in candidate_meta:
                            candidate_meta[cons] = {
                                "lift": r.get("lift", 1.0),
                                "triggered_by": source_item,
                            }
                except json.JSONDecodeError:
                    continue
        else:
            # Local fallback lookup
            for item_name in cart_item_names:
                rules = self.local_rules.get(item_name, [])
                for r in rules:
                    cons = r.get("consequent", "").strip().lower()
                    if not cons or cons in cart_item_names:
                        continue
                    conf = float(r.get("confidence", 0.0))
                    candidate_scores[cons] = candidate_scores.get(cons, 0.0) + conf
                    if cons not in candidate_meta:
                        candidate_meta[cons] = {
                            "lift": r.get("lift", 1.0),
                            "triggered_by": item_name,
                        }

        ranked_items = sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

        return [
            {
                "item_name": item,
                "score": round(score, 4),
                "lift": candidate_meta.get(item, {}).get("lift", 1.0),
                "triggered_by": candidate_meta.get(item, {}).get("triggered_by", ""),
                "type": "fp_growth_cart",
            }
            for item, score in ranked_items
        ]

    def recommend_for_item(self, item_name: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Direct single-item lookup for frequent companion products."""
        clean_key = item_name.strip().lower()
        if self.redis:
            key = f"rules:fpgrowth:{clean_key}"
            raw = self.redis.get(key)
            if not raw:
                return []
            try:
                rules = json.loads(raw)
            except json.JSONDecodeError:
                return []
        else:
            rules = self.local_rules.get(clean_key, [])

        return [
            {
                "item_name": r.get("consequent"),
                "score": round(float(r.get("confidence", 0.0)), 4),
                "lift": round(float(r.get("lift", 1.0)), 4),
                "type": "fp_growth_item",
            }
            for r in rules[:top_k]
        ]

    def recommend_for_user(self, user_id: int, top_k: int = 5) -> List[Dict[str, Any]]:
        """Queries Redis cache or executes on-the-fly PyTorch NCF inference."""
        # 1. Try Redis Feature Store Cache first (Sub-1ms)
        if self.redis:
            cached = self.redis.get(f"ncf:user:{user_id}")
            if cached:
                try:
                    recs = json.loads(cached)
                    return recs[:top_k]
                except json.JSONDecodeError:
                    pass

        # 2. On-the-fly Neural Collaborative Filtering Inference
        if self.ncf_model and user_id in self.user2idx:
            u_idx_val = self.user2idx[user_id]
            total_items = len(self.item2idx)
            u_tensor = torch.full((total_items,), u_idx_val, dtype=torch.long)
            i_tensor = torch.arange(total_items, dtype=torch.long)

            with torch.inference_mode():
                scores = self.ncf_model(u_tensor, i_tensor).detach().numpy()

            top_indices = torch.topk(torch.from_numpy(scores), top_k).indices.tolist()
            return [
                {
                    "item_name": self.idx2item[i],
                    "score": round(float(scores[i]), 4),
                    "type": "ncf_personalized",
                }
                for i in top_indices
                if i in self.idx2item
            ]

        return []

    def recommend_hybrid(
        self,
        user_id: int,
        session_id: Optional[str] = None,
        top_k: int = 5,
        alpha: float = 0.6,
    ) -> Dict[str, Any]:
        """
        Hybrid Ensemble:
        Blends short-term immediate basket intent (weight: alpha)
        with long-term user taste profile (weight: 1 - alpha).
        """
        start_time = time.perf_counter()

        cart_recs: List[Dict[str, Any]] = []
        if session_id:
            cart_recs = self.recommend_for_cart(session_id, top_k=top_k)

        user_recs = self.recommend_for_user(user_id, top_k=top_k)

        # Merge and rank
        merged: Dict[str, float] = {}
        for cr in cart_recs:
            merged[cr["item_name"]] = merged.get(cr["item_name"], 0.0) + (cr["score"] * alpha)

        for ur in user_recs:
            merged[ur["item_name"]] = merged.get(ur["item_name"], 0.0) + (ur["score"] * (1.0 - alpha))

        sorted_recs = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:top_k]

        duration_ms = (time.perf_counter() - start_time) * 1000

        return {
            "user_id": user_id,
            "session_id": session_id,
            "latency_ms": round(duration_ms, 2),
            "recommendations": [
                {"item_name": item, "combined_score": round(score, 4)}
                for item, score in sorted_recs
            ],
            "debug": {
                "cart_signals_count": len(cart_recs),
                "user_signals_count": len(user_recs),
            },
        }

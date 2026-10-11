# filepath: mlops/sync_redis_store.py
"""
Senior Data Engineer & MLOps Production Module:
Redis Feature Store Synchronizer.

Synchronizes offline batch artifacts into low-latency Redis Key-Value store:
1. FP-Growth Association Rules:
   - Key: 'rules:fpgrowth:{item_name}' -> JSON array of ranked recommendations.
   - Set: 'rules:all_items' -> Set of all available items with rules.
2. NCF Deep Learning User Recommendations:
   - Key: 'ncf:user:{user_id}' -> JSON array of top-K personalized item recommendations.
3. Batch pipelining for sub-second synchronization of thousands of items.
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import redis
import torch

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SyncRedisStore")


class NCFInferenceHelper:
    """Helper to load PyTorch NCF model checkpoint and precompute user rankings."""

    def __init__(self, checkpoint_path: Path):
        self.checkpoint_path = checkpoint_path
        self.user2idx: Dict[int, int] = {}
        self.item2idx: Dict[str, int] = {}
        self.idx2item: Dict[int, str] = {}
        self.model: Optional[torch.nn.Module] = None
        self._load()

    def _load(self) -> None:
        if not self.checkpoint_path.exists():
            logger.warning(f"NCF checkpoint not found at {self.checkpoint_path}. Skipping NCF precomputations.")
            return

        logger.info(f"Loading NCF checkpoint from: {self.checkpoint_path}")
        ckpt = torch.load(self.checkpoint_path, map_location="cpu", weights_only=False)

        for k, v in ckpt.get("user2idx", {}).items():
            try:
                self.user2idx[int(k)] = int(v)
            except ValueError:
                continue

        self.item2idx = {str(k): int(v) for k, v in ckpt.get("item2idx", {}).items()}
        self.idx2item = {v: k for k, v in self.item2idx.items()}

        # Instantiate NCF architecture matching existing model
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
        self.model = NCF(n_users, n_items).eval()
        self.model.load_state_dict(ckpt["model"])
        logger.info(f"Loaded NCF model: {n_users:,} users, {n_items:,} items.")

    @torch.inference_mode()
    def get_top_k_for_user(self, user_id: int, top_k: int = 10) -> List[Dict[str, Any]]:
        if not self.model or user_id not in self.user2idx:
            return []

        u_idx_val = self.user2idx[user_id]
        total_items = len(self.item2idx)
        u_idx = torch.full((total_items,), u_idx_val, dtype=torch.long)
        i_idx = torch.arange(total_items, dtype=torch.long)

        scores = self.model(u_idx, i_idx)
        scores_np = scores.detach().numpy()
        top_indices = torch.topk(scores, top_k).indices.tolist()

        return [
            {"item": self.idx2item[i], "score": round(float(scores_np[i]), 4)}
            for i in top_indices
            if i in self.idx2item
        ]


class RedisFeatureStoreSync:
    """Manages bulk pipeline synchronization to Redis Feature Store."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: Optional[str] = None):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            socket_timeout=10,
        )
        self.client.ping()
        logger.info(f"Connected to Redis Feature Store at {host}:{port}/{db}")

    def sync_fpgrowth_rules(self, rules_json_path: Path, fallback_csv_path: Path) -> int:
        """Loads and syncs FP-Growth rules to Redis Key-Value pairs."""
        lookup_data: Dict[str, List[Dict[str, Any]]] = {}

        if rules_json_path.exists():
            logger.info(f"Loading rules from JSON: {rules_json_path}")
            with open(rules_json_path, "r", encoding="utf-8") as f:
                lookup_data = json.load(f)
        elif fallback_csv_path.exists():
            logger.info(f"Falling back to rules CSV: {fallback_csv_path}")
            df = pd.read_csv(fallback_csv_path)

            def parse_item(val: Any) -> str:
                try:
                    p = ast.literal_eval(str(val))
                    if isinstance(p, (list, set)):
                        return str(next(iter(p))).strip().lower()
                except Exception:
                    pass
                return str(val).strip().lower()

            for _, row in df.iterrows():
                ante = parse_item(row["antecedent"])
                cons = parse_item(row["consequent"])
                conf = round(float(row["confidence"]), 4)
                lift = round(float(row.get("lift", 1.0)), 4)
                lookup_data.setdefault(ante, []).append({
                    "consequent": cons,
                    "confidence": conf,
                    "lift": lift,
                })
        else:
            raise FileNotFoundError("Neither rules JSON nor CSV found.")

        logger.info(f"Synchronizing {len(lookup_data):,} FP-Growth items to Redis via Pipeline...")
        pipe = self.client.pipeline()
        batch_size = 1000
        synced_count = 0

        # Clear old rule set
        self.client.delete("rules:all_items")

        for item_name, suggestions in lookup_data.items():
            key = f"rules:fpgrowth:{item_name.strip().lower()}"
            pipe.set(key, json.dumps(suggestions, ensure_ascii=False))
            pipe.sadd("rules:all_items", item_name.strip().lower())
            synced_count += 1

            if synced_count % batch_size == 0:
                pipe.execute()
                pipe = self.client.pipeline()

        pipe.execute()
        logger.info(f"Successfully synced {synced_count:,} FP-Growth rule entries to Redis.")
        return synced_count

    def sync_ncf_recommendations(self, ncf_helper: NCFInferenceHelper, max_users: Optional[int] = 2000, top_k: int = 10) -> int:
        """Precomputes and caches top-K personalized recommendations for users."""
        if not ncf_helper.model:
            return 0

        user_ids = list(ncf_helper.user2idx.keys())
        if max_users:
            user_ids = user_ids[:max_users]

        logger.info(f"Precomputing and syncing NCF recommendations for {len(user_ids):,} users...")
        pipe = self.client.pipeline()
        batch_size = 500
        synced_count = 0

        for uid in user_ids:
            recs = ncf_helper.get_top_k_for_user(uid, top_k=top_k)
            if recs:
                key = f"ncf:user:{uid}"
                pipe.set(key, json.dumps(recs, ensure_ascii=False))
                synced_count += 1

                if synced_count % batch_size == 0:
                    pipe.execute()
                    pipe = self.client.pipeline()

        pipe.execute()
        logger.info(f"Successfully cached NCF recommendations for {synced_count:,} users in Redis.")
        return synced_count

    def verify_sync(self) -> None:
        """Sanity check verifying Redis store keys."""
        all_rules_count = self.client.scard("rules:all_items")
        logger.info(f"[VERIFICATION] 'rules:all_items' total keys: {all_rules_count:,}")

        sample_items = self.client.srandmember("rules:all_items", 2)
        for it in sample_items:
            data = self.client.get(f"rules:fpgrowth:{it}")
            logger.info(f"[VERIFICATION SAMPLE] Item '{it}': {data[:120] if data else 'None'}...")


def main():
    parser = argparse.ArgumentParser(description="Sync FP-Growth Rules and NCF Embeddings to Redis Feature Store.")
    parser.add_argument("--rules-json", type=str, default="data/models/fpgrowth_rules.json", help="Path to rules JSON")
    parser.add_argument("--rules-csv", type=str, default="data/rules.csv", help="Fallback rules CSV")
    parser.add_argument("--ncf-model", type=str, default="models/ncf_model.pt", help="Path to NCF checkpoint")
    parser.add_argument("--redis-host", type=str, default=os.getenv("REDIS_HOST", "localhost"), help="Redis Host")
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")), help="Redis Port")
    parser.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", "0")), help="Redis DB")
    parser.add_argument("--max-users", type=int, default=2000, help="Max users to cache NCF recommendations for")

    args = parser.parse_args()

    start_time = time.time()
    sync_engine = RedisFeatureStoreSync(host=args.redis_host, port=args.redis_port, db=args.redis_db)

    # 1. Sync FP-Growth Rules
    sync_engine.sync_fpgrowth_rules(Path(args.rules_json), Path(args.rules_csv))

    # 2. Sync NCF Recommendations
    ncf_helper = NCFInferenceHelper(Path(args.ncf_model))
    sync_engine.sync_ncf_recommendations(ncf_helper, max_users=args.max_users)

    # 3. Verify
    sync_engine.verify_sync()
    logger.info(f"Feature Store Sync finished in {time.time() - start_time:.2f}s.")


if __name__ == "__main__":
    main()

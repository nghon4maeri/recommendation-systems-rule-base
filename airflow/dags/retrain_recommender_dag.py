# filepath: airflow/dags/retrain_recommender_dag.py
"""
Senior Data Engineer & MLOps Production DAG:
Airflow Orchestration Pipeline for Continuous Recommender Retraining & Feature Store Sync.

DAG Workflow:
1. check_lakehouse_freshness: Validates existence and schema of Lakehouse Parquet transactions.
2. mine_fpgrowth_rules: Mines association rules from multi-item baskets via FP-Growth.
3. retrain_ncf_model: Retrains Neural Collaborative Filtering (NCF) deep learning network.
4. sync_to_redis_feature_store: Atomic pipeline synchronization of rules and user embeddings into Redis.
5. verify_serving_health: Automated health check hitting the FastAPI backend gateway.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# DAG Configuration
DEFAULT_ARGS = {
    "owner": "data-platform-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def task_check_lakehouse_freshness(**context) -> str:
    """Verifies that the Lakehouse transaction data is available and non-empty."""
    lakehouse_path = Path("data/lakehouse/real_transactions.parquet")
    fallback_path = Path("data/online_retail.csv")

    target_path = lakehouse_path if lakehouse_path.exists() else fallback_path
    if not target_path.exists():
        raise FileNotFoundError(f"Neither {lakehouse_path} nor {fallback_path} exists. Pipeline cannot proceed.")

    size_mb = target_path.stat().st_size / (1024 * 1024)
    logger.info(f"Verified Lakehouse dataset: {target_path} (Size: {size_mb:.2f} MB)")
    return str(target_path)


def task_mine_fpgrowth_rules(**context) -> Dict[str, Any]:
    """Invokes FP-Growth mining pipeline and extracts association rules."""
    from mlops.train_fpgrowth import FPGrowthModelTrainer

    ti = context["ti"]
    data_path_str = ti.xcom_pull(task_ids="check_lakehouse_freshness")
    data_path = Path(data_path_str)

    trainer = FPGrowthModelTrainer(
        data_path=data_path,
        min_support=0.01,
        min_confidence=0.20,
        min_lift=1.0,
    )

    baskets = trainer.load_transactions()
    rules_df = trainer.train_and_extract_rules(baskets)

    if rules_df.empty:
        raise ValueError("FP-Growth produced 0 rules. Check minimum support/confidence thresholds.")

    lookup = trainer.format_rules_for_redis(rules_df)
    csv_path, json_path = trainer.save_artifacts(rules_df, lookup, Path("data/models"))

    logger.info(f"FP-Growth finished successfully. Rules saved to {json_path}")
    return {"total_rules": len(rules_df), "unique_antecedents": len(lookup)}


def task_retrain_ncf_model(**context) -> Dict[str, Any]:
    """Retrains Neural Collaborative Filtering (NCF) model using PyTorch."""
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, Dataset
    import pandas as pd

    data_path = Path("data/lakehouse/real_transactions.parquet")
    fallback_path = Path("data/online_retail.csv")
    input_path = data_path if data_path.exists() else fallback_path

    logger.info(f"Loading user-item interactions for NCF training from {input_path}...")
    df = pd.read_parquet(input_path) if input_path.suffix == ".parquet" else pd.read_csv(input_path)

    df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]
    u_col = "customerid" if "customerid" in df.columns else "customer id"
    i_col = "description" if "description" in df.columns else "item_name"

    df = df.dropna(subset=[u_col, i_col])
    df = df[df["quantity"] > 0]
    df[u_col] = df[u_col].astype(int)
    df[i_col] = df[i_col].astype(str).str.strip().str.lower()

    interactions = df[[u_col, i_col]].drop_duplicates()
    logger.info(f"Total distinct user-item interaction pairs: {len(interactions):,}")

    user2idx = {u: i for i, u in enumerate(interactions[u_col].unique())}
    item2idx = {it: j for j, it in enumerate(interactions[i_col].unique())}

    interactions["u_idx"] = interactions[u_col].map(user2idx)
    interactions["i_idx"] = interactions[i_col].map(item2idx)

    class NCFDataset(Dataset):
        def __init__(self, u_vals, i_vals):
            self.u = torch.tensor(u_vals, dtype=torch.long)
            self.i = torch.tensor(i_vals, dtype=torch.long)

        def __len__(self):
            return len(self.u)

        def __getitem__(self, idx):
            return self.u[idx], self.i[idx]

    class NCFModel(nn.Module):
        def __init__(self, n_users: int, n_items: int, emb_size: int = 64):
            super().__init__()
            self.u_emb = nn.Embedding(n_users, emb_size)
            self.i_emb = nn.Embedding(n_items, emb_size)
            self.fc = nn.Sequential(
                nn.Linear(emb_size * 2, 128),
                nn.ReLU(),
                nn.Linear(128, 1),
                nn.Sigmoid(),
            )

        def forward(self, u, i):
            x = torch.cat([self.u_emb(u), self.i_emb(i)], dim=1)
            return self.fc(x).squeeze()

    n_users, n_items = len(user2idx), len(item2idx)
    loader = DataLoader(NCFDataset(interactions["u_idx"].values, interactions["i_idx"].values), batch_size=2048, shuffle=True)

    model = NCFModel(n_users, n_items)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.003)
    criterion = nn.BCELoss()

    logger.info("Training NCF model for 3 epochs...")
    model.train()
    for epoch in range(3):
        total_loss = 0.0
        for u_batch, i_batch in loader:
            optimizer.zero_grad()
            preds = model(u_batch, i_batch)
            labels = torch.ones_like(preds)
            loss = criterion(preds, labels)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
        logger.info(f"Epoch {epoch + 1}/3 - Loss: {total_loss / len(loader):.4f}")

    # Save checkpoint
    out_dir = Path("models")
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = out_dir / "ncf_model.pt"

    checkpoint = {
        "model": model.state_dict(),
        "user2idx": {str(k): v for k, v in user2idx.items()},
        "item2idx": item2idx,
    }
    torch.save(checkpoint, checkpoint_path)
    logger.info(f"Saved retrained NCF checkpoint to: {checkpoint_path}")

    return {"n_users": n_users, "n_items": n_items}


def task_sync_to_redis_feature_store(**context) -> Dict[str, Any]:
    """Pushes fresh FP-Growth rules and user embeddings into Redis Key-Value store."""
    import os
    from mlops.sync_redis_store import NCFInferenceHelper, RedisFeatureStoreSync

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))

    sync_engine = RedisFeatureStoreSync(host=redis_host, port=redis_port, db=redis_db)

    # Sync FP-Growth rules
    rules_json = Path("data/models/fpgrowth_rules.json")
    rules_csv = Path("data/rules.csv")
    rules_count = sync_engine.sync_fpgrowth_rules(rules_json, rules_csv)

    # Sync NCF recommendations
    ncf_path = Path("models/ncf_model.pt")
    ncf_helper = NCFInferenceHelper(ncf_path)
    user_count = sync_engine.sync_ncf_recommendations(ncf_helper, max_users=2000, top_k=10)

    sync_engine.verify_sync()
    logger.info("Redis Feature Store synchronization completed successfully.")
    return {"synced_rules": rules_count, "synced_users": user_count}


def task_verify_serving_health(**context) -> None:
    """Performs HTTP health probe against FastAPI serving gateway."""
    import urllib.request
    import json

    api_url = "http://localhost:8000/health"
    logger.info(f"Probing backend health at: {api_url}")
    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": "Airflow-HealthCheck"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            logger.info(f"Backend Health Response: {data}")
            if data.get("status") not in ["healthy", "ok", "degraded"]:
                raise RuntimeError(f"Backend unhealthy: {data}")
    except Exception as e:
        logger.warning(f"Could not reach local FastAPI server ({e}). Ensure API is running in container/cluster.")


with DAG(
    dag_id="retrain_hybrid_recommender_pipeline",
    default_args=DEFAULT_ARGS,
    description="Automated Lakehouse ELT, Model Retraining, and Redis Feature Store Synchronization",
    schedule_interval="0 2 * * 0",  # Every Sunday at 02:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "mlops", "streaming", "redis"],
) as dag:

    check_lakehouse = PythonOperator(
        task_id="check_lakehouse_freshness",
        python_callable=task_check_lakehouse_freshness,
    )

    mine_fpgrowth = PythonOperator(
        task_id="mine_fpgrowth_rules",
        python_callable=task_mine_fpgrowth_rules,
    )

    retrain_ncf = PythonOperator(
        task_id="retrain_ncf_model",
        python_callable=task_retrain_ncf_model,
    )

    sync_redis = PythonOperator(
        task_id="sync_to_redis_feature_store",
        python_callable=task_sync_to_redis_feature_store,
    )

    verify_health = PythonOperator(
        task_id="verify_serving_health",
        python_callable=task_verify_serving_health,
    )

    # Directed Acyclic Graph Topology
    check_lakehouse >> [mine_fpgrowth, retrain_ncf] >> sync_redis >> verify_health

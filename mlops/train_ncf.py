# filepath: mlops/train_ncf.py
"""
Senior Data Engineer & MLOps Production Module:
PyTorch Neural Collaborative Filtering (NCF) Training Pipeline.

Architecture:
1. Ingestion: Reads user-item interactions from Lakehouse Parquet or CSV.
2. Negative Sampling: Generates unobserved negative interaction pairs for implicit feedback.
3. Neural Architecture: Dual embedding layers (User & Item) fed into Multi-Layer Perceptron (MLP).
4. Training Engine: Mini-batch optimization via BCEWithLogitsLoss / BCELoss and Adam optimizer.
5. Model Registry & Checkpointing: Serializes state_dict and user/item integer mappings to disk.
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("TrainNCF")


class InteractionDataset(Dataset):
    """PyTorch Dataset for (user_idx, item_idx, label) triplets."""

    def __init__(self, users: np.ndarray, items: np.ndarray, labels: np.ndarray):
        self.users = torch.tensor(users, dtype=torch.long)
        self.items = torch.tensor(items, dtype=torch.long)
        self.labels = torch.tensor(labels, dtype=torch.float32)

    def __len__(self) -> int:
        return len(self.users)

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return self.users[idx], self.items[idx], self.labels[idx]


class NeuralCollaborativeFiltering(nn.Module):
    """Deep Neural Collaborative Filtering Network with dense embeddings and MLP layers."""

    def __init__(self, num_users: int, num_items: int, emb_size: int = 64, hidden_dim: int = 128):
        super().__init__()
        self.num_users = num_users
        self.num_items = num_items
        self.emb_size = emb_size

        self.u_emb = nn.Embedding(num_users, emb_size)
        self.i_emb = nn.Embedding(num_items, emb_size)

        self.fc = nn.Sequential(
            nn.Linear(emb_size * 2, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid(),
        )

        self._init_weights()

    def _init_weights(self) -> None:
        nn.init.normal_(self.u_emb.weight, std=0.01)
        nn.init.normal_(self.i_emb.weight, std=0.01)
        for layer in self.fc:
            if isinstance(layer, nn.Linear):
                nn.init.xavier_uniform_(layer.weight)
                nn.init.zeros_(layer.bias)

    def forward(self, u: torch.Tensor, i: torch.Tensor) -> torch.Tensor:
        u_lat = self.u_emb(u)
        i_lat = self.i_emb(i)
        x = torch.cat([u_lat, i_lat], dim=-1)
        return self.fc(x).squeeze(-1)


class NCFTrainingPipeline:
    """End-to-End NCF Training, Evaluation and Serialization Pipeline."""

    def __init__(
        self,
        data_path: Path,
        emb_size: int = 64,
        hidden_dim: int = 128,
        batch_size: int = 2048,
        learning_rate: float = 0.003,
        epochs: int = 5,
        neg_ratio: int = 2,
        device: Optional[str] = None,
    ):
        self.data_path = data_path
        self.emb_size = emb_size
        self.hidden_dim = hidden_dim
        self.batch_size = batch_size
        self.lr = learning_rate
        self.epochs = epochs
        self.neg_ratio = neg_ratio

        if device:
            self.device = torch.device(device)
        else:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.user2idx: Dict[int, int] = {}
        self.item2idx: Dict[str, int] = {}
        self.idx2user: Dict[int, int] = {}
        self.idx2item: Dict[int, str] = {}
        self.model: Optional[NeuralCollaborativeFiltering] = None

    def load_interactions(self) -> pd.DataFrame:
        """Loads and cleans interaction logs from Parquet or CSV."""
        logger.info(f"Loading interaction data from: {self.data_path}")
        if self.data_path.suffix == ".parquet":
            df = pd.read_parquet(self.data_path)
        else:
            try:
                df = pd.read_csv(self.data_path, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(self.data_path, encoding="ISO-8859-1")

        df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]

        u_col = "customerid" if "customerid" in df.columns else "user_id"
        i_col = "description" if "description" in df.columns else "item_name"
        if i_col not in df.columns and "item_id" in df.columns:
            i_col = "item_id"

        df = df.dropna(subset=[u_col, i_col])
        if "quantity" in df.columns:
            df = df[df["quantity"] > 0]

        df[u_col] = df[u_col].astype(int)
        df[i_col] = df[i_col].astype(str).str.strip().str.lower()

        interactions = df[[u_col, i_col]].drop_duplicates().reset_index(drop=True)
        interactions.columns = ["user_id", "item_name"]
        logger.info(f"Loaded {len(interactions):,} unique (user, item) interaction pairs.")
        return interactions

    def build_mappings(self, df: pd.DataFrame) -> None:
        """Builds contiguous integer ID lookup dictionaries for users and items."""
        unique_users = sorted(df["user_id"].unique())
        unique_items = sorted(df["item_name"].unique())

        self.user2idx = {u: idx for idx, u in enumerate(unique_users)}
        self.item2idx = {it: idx for idx, it in enumerate(unique_items)}
        self.idx2user = {idx: u for u, idx in self.user2idx.items()}
        self.idx2item = {idx: it for it, idx in self.item2idx.items()}

        logger.info(f"Vocabulary indexed: {len(self.user2idx):,} users, {len(self.item2idx):,} items.")

    def generate_training_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Generates positive observations and sampled negative unobserved items."""
        logger.info(f"Generating training dataset with negative sampling ratio {self.neg_ratio}:1...")
        num_items = len(self.item2idx)
        user_positives: Dict[int, Set[int]] = {}

        u_indices = df["user_id"].map(self.user2idx).values
        i_indices = df["item_name"].map(self.item2idx).values

        for u_idx, i_idx in zip(u_indices, i_indices):
            user_positives.setdefault(u_idx, set()).add(i_idx)

        all_u: List[int] = []
        all_i: List[int] = []
        all_y: List[float] = []

        # Add positive examples
        for u_idx, pos_items in user_positives.items():
            for i_pos in pos_items:
                all_u.append(u_idx)
                all_i.append(i_pos)
                all_y.append(1.0)

                # Sample negative items
                for _ in range(self.neg_ratio):
                    neg_candidate = random.randint(0, num_items - 1)
                    while neg_candidate in pos_items:
                        neg_candidate = random.randint(0, num_items - 1)
                    all_u.append(u_idx)
                    all_i.append(neg_candidate)
                    all_y.append(0.0)

        users_arr = np.array(all_u, dtype=np.int64)
        items_arr = np.array(all_i, dtype=np.int64)
        labels_arr = np.array(all_y, dtype=np.float32)

        # Shuffle pairs
        perm = np.random.permutation(len(users_arr))
        logger.info(f"Total training samples generated: {len(users_arr):,} (Positive: {len(df):,})")
        return users_arr[perm], items_arr[perm], labels_arr[perm]

    def train(self, users: np.ndarray, items: np.ndarray, labels: np.ndarray) -> NeuralCollaborativeFiltering:
        """Trains the NCF model over specified epochs."""
        num_users = len(self.user2idx)
        num_items = len(self.item2idx)

        self.model = NeuralCollaborativeFiltering(
            num_users=num_users,
            num_items=num_items,
            emb_size=self.emb_size,
            hidden_dim=self.hidden_dim,
        ).to(self.device)

        dataset = InteractionDataset(users, items, labels)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True, drop_last=False)

        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr, weight_decay=1e-5)
        criterion = nn.BCELoss()

        logger.info(f"Starting training on device: {self.device} for {self.epochs} epochs...")
        start_time = time.time()

        for epoch in range(1, self.epochs + 1):
            self.model.train()
            epoch_loss = 0.0
            total_batches = len(loader)

            for u_batch, i_batch, y_batch in loader:
                u_batch = u_batch.to(self.device)
                i_batch = i_batch.to(self.device)
                y_batch = y_batch.to(self.device)

                optimizer.zero_grad()
                preds = self.model(u_batch, i_batch)
                loss = criterion(preds, y_batch)
                loss.backward()
                optimizer.step()

                epoch_loss += loss.item()

            avg_loss = epoch_loss / max(1, total_batches)
            logger.info(f"Epoch [{epoch:02d}/{self.epochs:02d}] - Average BCE Loss: {avg_loss:.4f}")

        elapsed = time.time() - start_time
        logger.info(f"Training completed in {elapsed:.2f}s ({elapsed / self.epochs:.2f}s per epoch).")
        return self.model

    def save_checkpoint(self, output_path: Path) -> Path:
        """Saves model weights and token dictionaries in standard checkpoint format."""
        if self.model is None:
            raise RuntimeError("Cannot save model: training has not been executed.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint = {
            "model": self.model.cpu().state_dict(),
            "user2idx": {str(k): v for k, v in self.user2idx.items()},
            "item2idx": {str(k): v for k, v in self.item2idx.items()},
            "metadata": {
                "emb_size": self.emb_size,
                "hidden_dim": self.hidden_dim,
                "num_users": len(self.user2idx),
                "num_items": len(self.item2idx),
                "timestamp": time.time(),
            },
        }

        torch.save(checkpoint, output_path)
        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Saved NCF model checkpoint to: {output_path} (Size: {size_mb:.2f} MB)")
        return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="PyTorch Neural Collaborative Filtering (NCF) Model Trainer")
    parser.add_argument(
        "--data-path",
        type=Path,
        default=Path("data/lakehouse/real_transactions.parquet"),
        help="Path to Lakehouse Parquet or CSV transactions",
    )
    parser.add_argument(
        "--output-model",
        type=Path,
        default=Path("models/ncf_model.pt"),
        help="Target output checkpoint path",
    )
    parser.add_argument("--epochs", type=int, default=3, help="Number of training epochs")
    parser.add_argument("--batch-size", type=int, default=2048, help="DataLoader batch size")
    parser.add_argument("--emb-size", type=int, default=64, help="Embedding dimension")
    parser.add_argument("--lr", type=float, default=0.003, help="Learning rate")
    args = parser.parse_args()

    # Fallback path if target parquet doesn't exist yet
    data_file = args.data_path
    if not data_file.exists():
        fallback = Path("data/online_retail.csv")
        if fallback.exists():
            data_file = fallback
            logger.warning(f"Target data {args.data_path} not found. Falling back to {fallback}")
        else:
            raise FileNotFoundError(f"No valid transaction data found at {args.data_path} or {fallback}")

    pipeline = NCFTrainingPipeline(
        data_path=data_file,
        emb_size=args.emb_size,
        batch_size=args.batch_size,
        learning_rate=args.lr,
        epochs=args.epochs,
    )

    df_interactions = pipeline.load_interactions()
    pipeline.build_mappings(df_interactions)
    u_data, i_data, y_data = pipeline.generate_training_data(df_interactions)
    pipeline.train(u_data, i_data, y_data)
    pipeline.save_checkpoint(args.output_model)


if __name__ == "__main__":
    main()

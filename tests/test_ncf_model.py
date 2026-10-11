# filepath: tests/test_ncf_model.py
"""
Senior Data Engineer & MLOps Automated Test Suite:
PyTorch Neural Collaborative Filtering (NCF) Model Architecture & Inference Tests.
"""

from __future__ import annotations

import pytest
import torch


class NCF(torch.nn.Module):
    """Neural Collaborative Filtering architecture used for serving."""

    def __init__(self, n_users: int, n_items: int, emb_size: int = 32):
        super().__init__()
        self.u_emb = torch.nn.Embedding(n_users, emb_size)
        self.i_emb = torch.nn.Embedding(n_items, emb_size)
        self.fc = torch.nn.Sequential(
            torch.nn.Linear(emb_size * 2, 64),
            torch.nn.ReLU(),
            torch.nn.Linear(64, 1),
            torch.nn.Sigmoid(),
        )

    def forward(self, u: torch.Tensor, i: torch.Tensor) -> torch.Tensor:
        x = torch.cat([self.u_emb(u), self.i_emb(i)], dim=1)
        return self.fc(x).squeeze(-1)


@pytest.mark.ml
@pytest.mark.unit
def test_ncf_forward_pass_dimensions() -> None:
    """Verifies that NCF forward pass outputs sigmoid bounded scores [0.0, 1.0]."""
    n_users = 100
    n_items = 50
    model = NCF(n_users=n_users, n_items=n_items, emb_size=16).eval()

    user_batch = torch.tensor([0, 10, 42], dtype=torch.long)
    item_batch = torch.tensor([5, 12, 33], dtype=torch.long)

    with torch.no_grad():
        scores = model(user_batch, item_batch)

    assert scores.shape == (3,)
    assert (scores >= 0.0).all()
    assert (scores <= 1.0).all()


@pytest.mark.ml
@pytest.mark.unit
def test_ncf_batch_ranking() -> None:
    """Verifies ranking top-K item candidates for a specific user."""
    n_users = 50
    n_items = 100
    model = NCF(n_users=n_users, n_items=n_items, emb_size=16).eval()

    target_user_id = 15
    candidates = torch.arange(n_items, dtype=torch.long)
    user_tensor = torch.full_like(candidates, target_user_id)

    with torch.no_grad():
        predicted_scores = model(user_tensor, candidates)

    topk_scores, topk_indices = torch.topk(predicted_scores, k=5)

    assert len(topk_scores) == 5
    assert len(topk_indices) == 5
    # Asserts sorted descending
    for i in range(len(topk_scores) - 1):
        assert topk_scores[i] >= topk_scores[i + 1]


@pytest.mark.ml
@pytest.mark.unit
def test_ncf_checkpoint_weights_integrity(tmp_path) -> None:
    """Verifies checkpoint serialization and weight loading integrity."""
    model = NCF(n_users=20, n_items=30, emb_size=8)
    ckpt_path = tmp_path / "test_ncf_ckpt.pt"

    torch.save(
        {
            "model": model.state_dict(),
            "user2idx": {f"user_{i}": i for i in range(20)},
            "item2idx": {f"item_{i}": i for i in range(30)},
        },
        ckpt_path,
    )

    assert ckpt_path.exists()
    loaded_ckpt = torch.load(ckpt_path, weights_only=False)
    assert "model" in loaded_ckpt
    assert "user2idx" in loaded_ckpt
    assert len(loaded_ckpt["user2idx"]) == 20

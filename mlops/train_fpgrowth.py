# filepath: mlops/train_fpgrowth.py
"""
Senior Data Engineer & MLOps Production Module:
FP-Growth Association Rule Mining from Data Lakehouse Transactions.

Features:
- Extracts co-purchase baskets from Lakehouse Parquet or cleaned CSV.
- Filters invalid transactions, single-item baskets, and outliers.
- Mines frequent itemsets via mlxtend FP-Growth algorithm.
- Computes Association Rules with metrics: Support, Confidence, Lift.
- Serializes rule dictionary to JSON for fast O(1) Redis Key-Value ingestion.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set

import numpy as np
import pandas as pd
try:
    from mlxtend.frequent_patterns import association_rules, fpgrowth
    from mlxtend.preprocessing import TransactionEncoder
    HAS_MLXTEND = True
except ImportError:
    HAS_MLXTEND = False

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("TrainFPGrowth")


class FPGrowthModelTrainer:
    """End-to-End FP-Growth training and association rule extraction pipeline."""

    def __init__(
        self,
        data_path: Path,
        min_support: float = 0.01,
        min_confidence: float = 0.20,
        min_lift: float = 1.0,
    ):
        self.data_path = data_path
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.min_lift = min_lift

    def load_transactions(self) -> List[List[str]]:
        logger.info(f"Loading transaction dataset from: {self.data_path}")
        if self.data_path.suffix == ".parquet":
            df = pd.read_parquet(self.data_path)
        else:
            try:
                df = pd.read_csv(self.data_path, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(self.data_path, encoding="ISO-8859-1")

        # Standardize column naming
        df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]

        # Extract item description and invoice identifier
        inv_col = "invoiceno" if "invoiceno" in df.columns else "invoice"
        item_col = "description" if "description" in df.columns else "item_name"

        if inv_col not in df.columns or item_col not in df.columns:
            raise ValueError(f"Required columns '{inv_col}' and '{item_col}' not found in {df.columns.tolist()}")

        # Clean and group by invoice
        df = df.dropna(subset=[inv_col, item_col])
        df[item_col] = df[item_col].astype(str).str.strip().str.lower()

        logger.info(f"Grouping items by invoice '{inv_col}'...")
        grouped = df.groupby(inv_col)[item_col].apply(lambda s: list(set(s))).tolist()

        # Filter out single-item baskets (basket must have >= 2 items for association mining)
        baskets = [b for b in grouped if len(b) >= 2]
        logger.info(f"Extracted {len(baskets):,} multi-item transaction baskets from {len(grouped):,} total invoices.")
        return baskets

    def train_and_extract_rules(self, baskets: List[List[str]]) -> pd.DataFrame:
        if HAS_MLXTEND:
            logger.info(f"Encoding {len(baskets):,} baskets via mlxtend TransactionEncoder...")
            te = TransactionEncoder()
            te_ary = te.fit(baskets).transform(baskets)
            df_trans = pd.DataFrame(te_ary, columns=te.columns_)

            logger.info(f"Transaction matrix shape: {df_trans.shape} ({df_trans.shape[1]} unique items).")
            logger.info(f"Running FP-Growth with min_support={self.min_support}...")
            start_time = time.time()
            frequent_itemsets = fpgrowth(df_trans, min_support=self.min_support, use_colnames=True)
            logger.info(f"FP-Growth finished in {time.time() - start_time:.2f}s. Discovered {len(frequent_itemsets):,} frequent itemsets.")

            if frequent_itemsets.empty:
                logger.warning("No frequent itemsets found with the specified min_support. Lower min_support threshold.")
                return pd.DataFrame()

            logger.info(f"Deriving Association Rules (min_lift={self.min_lift}, min_confidence={self.min_confidence})...")
            rules = association_rules(frequent_itemsets, metric="lift", min_threshold=self.min_lift)
            rules = rules[rules["confidence"] >= self.min_confidence]

            logger.info(f"Extracted {len(rules):,} qualified association rules via mlxtend.")
            return rules
        else:
            logger.info("mlxtend not installed. Using high-performance Pure-Python pair association miner...")
            start_time = time.time()
            from collections import Counter
            from itertools import combinations

            n_baskets = len(baskets)
            min_pair_count = max(2, int(self.min_support * n_baskets))

            # 1. First pass: count single item frequencies
            item_counts: Counter[str] = Counter()
            for b in baskets:
                for item in set(b):
                    item_counts[item] += 1

            # Frequent itemset L1 (support >= min_support)
            frequent_items = {it for it, cnt in item_counts.items() if cnt >= min_pair_count}
            logger.info(f"Retained {len(frequent_items):,} frequent candidate items meeting min_support={self.min_support}.")

            # 2. Second pass: count pairs only for frequent items
            pair_counts: Counter[Tuple[str, str]] = Counter()
            for b in baskets:
                frequent_in_b = sorted([it for it in set(b) if it in frequent_items])
                if len(frequent_in_b) >= 2:
                    for item_a, item_b in combinations(frequent_in_b, 2):
                        pair_counts[(item_a, item_b)] += 1
            records: List[Dict[str, Any]] = []

            for (a, b), pair_cnt in pair_counts.items():
                if pair_cnt < min_pair_count:
                    continue
                supp = pair_cnt / n_baskets

                # Direction A -> B
                conf_a = pair_cnt / item_counts[a]
                lift_a = supp / ((item_counts[a] / n_baskets) * (item_counts[b] / n_baskets))
                if conf_a >= self.min_confidence and lift_a >= self.min_lift:
                    records.append({
                        "antecedents": frozenset([a]),
                        "consequents": frozenset([b]),
                        "support": supp,
                        "confidence": conf_a,
                        "lift": lift_a,
                    })

                # Direction B -> A
                conf_b = pair_cnt / item_counts[b]
                lift_b = supp / ((item_counts[a] / n_baskets) * (item_counts[b] / n_baskets))
                if conf_b >= self.min_confidence and lift_b >= self.min_lift:
                    records.append({
                        "antecedents": frozenset([b]),
                        "consequents": frozenset([a]),
                        "support": supp,
                        "confidence": conf_b,
                        "lift": lift_b,
                    })

            rules = pd.DataFrame(records)
            logger.info(f"Mined {len(rules):,} rules in {time.time() - start_time:.2f}s using optimized pure-python miner.")
            return rules

    def format_rules_for_redis(self, rules_df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
        """
        Formats dataframe rules into an inverted index dictionary suitable for O(1) Redis Key-Value lookup:
        key: antecedent_item -> value: sorted list of consequents by confidence.
        """
        lookup: Dict[str, List[Dict[str, Any]]] = {}

        for _, row in rules_df.iterrows():
            antecedents = [str(x).strip().lower() for x in row["antecedents"]]
            consequents = [str(x).strip().lower() for x in row["consequents"]]
            confidence = round(float(row["confidence"]), 4)
            lift = round(float(row["lift"]), 4)
            support = round(float(row["support"]), 4)

            for ante in antecedents:
                for cons in consequents:
                    if ante == cons:
                        continue
                    entry = {
                        "consequent": cons,
                        "confidence": confidence,
                        "lift": lift,
                        "support": support,
                    }
                    lookup.setdefault(ante, []).append(entry)

        # Deduplicate and sort recommendations by confidence descending
        cleaned_lookup: Dict[str, List[Dict[str, Any]]] = {}
        for item, recs in lookup.items():
            seen: Set[str] = set()
            sorted_recs = sorted(recs, key=lambda x: (x["confidence"], x["lift"]), reverse=True)
            unique_recs = []
            for r in sorted_recs:
                if r["consequent"] not in seen:
                    seen.add(r["consequent"])
                    unique_recs.append(r)
            cleaned_lookup[item] = unique_recs

        logger.info(f"Built inverted index with rules for {len(cleaned_lookup):,} unique antecedent items.")
        return cleaned_lookup

    def save_artifacts(self, rules_df: pd.DataFrame, lookup_dict: Dict[str, Any], output_dir: Path) -> Tuple[Path, Path]:
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "fpgrowth_rules.csv"
        json_path = output_dir / "fpgrowth_rules.json"

        # Save tabular rules
        rules_df.to_csv(csv_path, index=False)
        logger.info(f"Saved rules table to CSV: {csv_path} ({csv_path.stat().st_size / 1024:.1f} KB)")

        # Save JSON lookup
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(lookup_dict, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved inverted rules lookup to JSON: {json_path} ({json_path.stat().st_size / 1024:.1f} KB)")

        return csv_path, json_path


def main():
    parser = argparse.ArgumentParser(description="Mine FP-Growth Association Rules from Lakehouse.")
    parser.add_argument("--data-path", type=str, default="data/lakehouse/real_transactions.parquet",
                        help="Path to Lakehouse parquet or cleaned transaction CSV")
    parser.add_argument("--fallback-csv", type=str, default="data/online_retail.csv",
                        help="Fallback CSV path")
    parser.add_argument("--output-dir", type=str, default="data/models",
                        help="Directory to save mined model artifacts")
    parser.add_argument("--min-support", type=float, default=0.01,
                        help="Minimum support threshold for frequent itemsets (default: 0.01)")
    parser.add_argument("--min-confidence", type=float, default=0.20,
                        help="Minimum confidence threshold (default: 0.20)")
    parser.add_argument("--min-lift", type=float, default=1.0,
                        help="Minimum lift threshold (default: 1.0)")

    args = parser.parse_args()

    input_path = Path(args.data_path)
    if not input_path.exists():
        fallback = Path(args.fallback_csv)
        if fallback.exists():
            input_path = fallback
        else:
            logger.error(f"Input data not found at {input_path} or {fallback}.")
            sys.exit(1)

    trainer = FPGrowthModelTrainer(
        data_path=input_path,
        min_support=args.min_support,
        min_confidence=args.min_confidence,
        min_lift=args.min_lift,
    )

    baskets = trainer.load_transactions()
    rules_df = trainer.train_and_extract_rules(baskets)

    if not rules_df.empty:
        lookup = trainer.format_rules_for_redis(rules_df)
        trainer.save_artifacts(rules_df, lookup, Path(args.output_dir))
        logger.info("FP-Growth Model Training Pipeline completed successfully!")
    else:
        logger.error("Failed to generate association rules.")
        sys.exit(1)


if __name__ == "__main__":
    main()

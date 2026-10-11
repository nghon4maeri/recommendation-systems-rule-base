# filepath: data_pipeline/ingest_kaggle_data.py
"""
data_pipeline/ingest_kaggle_data.py
Automated Kaggle Dataset Downloader & Real Data Ingestion Pipeline.

Downloads real e-commerce transaction datasets from Kaggle:
- Primary: 'mashlyn/online-retail-ii-uci' (1,065,000+ real transactions from 2009-2011)
- Fallback/Local: Automatically integrates existing 'data/online_retail.csv' (541,909 real transactions)
- Cleans and converts raw data into Partitioned Parquet format ready for Lakehouse / Kafka ingestion.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("KaggleIngestion")


class KaggleDownloader:
    """Handles automated authentication and downloading from Kaggle API."""

    DEFAULT_DATASET = "mashlyn/online-retail-ii-uci"

    def __init__(self, download_dir: Path):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def download_dataset(self, dataset_slug: str = DEFAULT_DATASET) -> Optional[Path]:
        logger.info(f"Initiating Kaggle download for dataset: {dataset_slug}")
        try:
            import subprocess
            cmd = [
                "kaggle", "datasets", "download",
                "-d", dataset_slug,
                "-p", str(self.download_dir),
                "--unzip"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Kaggle CLI download successful: {result.stdout.strip()}")
            
            csv_files = list(self.download_dir.glob("*.csv"))
            if csv_files:
                target_file = max(csv_files, key=lambda f: f.stat().st_size)
                logger.info(f"Downloaded real dataset: {target_file.name} ({target_file.stat().st_size / 1024 / 1024:.2f} MB)")
                return target_file

        except Exception as e:
            logger.warning(f"Kaggle CLI download failed or not configured: {e}")
            logger.info("Attempting to use kaggle Python API directly...")
            try:
                from kaggle.api.kaggle_api_extended import KaggleApi
                api = KaggleApi()
                api.authenticate()
                api.dataset_download_files(dataset_slug, path=str(self.download_dir), unzip=True)
                csv_files = list(self.download_dir.glob("*.csv"))
                if csv_files:
                    target_file = max(csv_files, key=lambda f: f.stat().st_size)
                    logger.info(f"Downloaded real dataset via Python API: {target_file.name}")
                    return target_file
            except Exception as py_e:
                logger.error(f"Kaggle API authentication error: {py_e}")

        return None


class RealDataProcessor:
    """Cleans, normalizes, and prepares real transaction data for DE pipelines & Kafka stream."""

    @staticmethod
    def load_and_standardize(file_path: Path) -> pd.DataFrame:
        logger.info(f"Loading real transaction dataset from: {file_path}")
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="ISO-8859-1")

        df.columns = [c.replace("\ufeff", "").strip() for c in df.columns]

        col_map = {
            "Invoice": "invoiceno",
            "InvoiceNo": "invoiceno",
            "StockCode": "stockcode",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoicedate",
            "Price": "unitprice",
            "UnitPrice": "unitprice",
            "Customer ID": "customerid",
            "CustomerID": "customerid",
            "Country": "country"
        }
        renamed = {}
        for c in df.columns:
            for k, v in col_map.items():
                if c.lower() == k.lower():
                    renamed[c] = v
                    break
        df = df.rename(columns=renamed)

        initial_len = len(df)
        df = df.dropna(subset=["invoiceno", "description", "customerid"])
        df = df[df["quantity"] > 0]
        df = df[df["unitprice"] > 0]
        df = df[~df["invoiceno"].astype(str).str.startswith("C")]

        df["customerid"] = df["customerid"].astype(int)
        df["invoicedate"] = pd.to_datetime(df["invoicedate"])
        df["description"] = df["description"].astype(str).str.strip().str.lower()
        df["event_date"] = df["invoicedate"].dt.strftime("%Y-%m-%d")

        df = df.sort_values("invoicedate").reset_index(drop=True)

        logger.info(f"Cleaned valid transactions: {len(df):,} rows from {initial_len:,} raw rows.")
        return df

    @staticmethod
    def export_lakehouse_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        partition_dir = output_dir / "transactions_partitioned"
        monolithic_file = output_dir / "real_transactions.parquet"

        logger.info(f"Exporting cleaned real data to Parquet at: {partition_dir}")
        df.to_parquet(partition_dir, partition_cols=["event_date"], index=False, engine="pyarrow")
        df.to_parquet(monolithic_file, index=False, engine="pyarrow")
        logger.info(f"Saved real dataset Parquet: {monolithic_file} ({monolithic_file.stat().st_size / 1024 / 1024:.2f} MB)")
        return monolithic_file


def main():
    parser = argparse.ArgumentParser(description="Ingest real e-commerce data from Kaggle.")
    parser.add_argument("--source", choices=["kaggle", "local", "auto"], default="auto")
    parser.add_argument("--dataset-slug", type=str, default="mashlyn/online-retail-ii-uci")
    parser.add_argument("--raw-dir", type=str, default="data/raw_kaggle")
    parser.add_argument("--output-dir", type=str, default="data/lakehouse")

    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    output_dir = Path(args.output_dir)
    local_existing = Path("data/online_retail.csv")

    target_csv: Optional[Path] = None

    if args.source in ["kaggle", "auto"]:
        downloader = KaggleDownloader(download_dir=raw_dir)
        target_csv = downloader.download_dataset(args.dataset_slug)

    if not target_csv and args.source in ["local", "auto"]:
        if local_existing.exists():
            target_csv = local_existing

    if not target_csv:
        logger.error("No dataset could be loaded.")
        sys.exit(1)

    processor = RealDataProcessor()
    df_clean = processor.load_and_standardize(target_csv)
    processor.export_lakehouse_parquet(df_clean, output_dir)
    logger.info("Real dataset ingestion completed successfully!")


if __name__ == "__main__":
    main()

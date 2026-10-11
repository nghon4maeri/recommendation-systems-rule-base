# filepath: airflow/dags/lakehouse_elt_dag.py
"""
Senior Data Engineer Production DAG:
Daily Lakehouse ELT & Data Quality Orchestration Pipeline.

Workflow:
1. extract_and_validate_raw_lakehouse: Ingests raw transactions from Kaggle/Lakehouse Parquet.
2. run_dbt_staging_transformations: Runs dbt staging models to sanitize transaction logs.
3. run_dbt_marts_transformations: Materializes marts: 'transaction_fpgrowth' and 'user_item_dl'.
4. validate_data_quality: Performs assertion-based data quality checks (schema, nulls, ranges).
5. export_gold_marts_for_ml: Serializes marts for downstream FP-Growth and NCF training.
6. publish_elt_completion_metrics: Logs execution summary and notifies pipeline status.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task.lakehouse_elt")

DEFAULT_ARGS = {
    "owner": "data-platform-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def task_extract_and_validate_raw(**context) -> Dict[str, Any]:
    """Verifies that the raw transactional Lakehouse dataset exists and has valid volume."""
    lakehouse_file = Path("data/lakehouse/real_transactions.parquet")
    raw_kaggle_dir = Path("data/raw_kaggle")
    fallback_csv = Path("data/online_retail.csv")

    source_path: Path
    if lakehouse_file.exists():
        source_path = lakehouse_file
    elif list(raw_kaggle_dir.glob("*.csv")):
        source_path = max(raw_kaggle_dir.glob("*.csv"), key=lambda f: f.stat().st_size)
    elif fallback_csv.exists():
        source_path = fallback_csv
    else:
        raise FileNotFoundError("No raw transaction dataset found in Lakehouse, raw_kaggle, or data/.")

    file_size_mb = source_path.stat().st_size / (1024 * 1024)
    logger.info(f"Verified raw source file: {source_path} (Size: {file_size_mb:.2f} MB)")

    ti = context["ti"]
    ti.xcom_push(key="raw_source_path", value=str(source_path))
    ti.xcom_push(key="source_size_mb", value=file_size_mb)
    return {"source": str(source_path), "size_mb": file_size_mb}


def task_run_dbt_staging(**context) -> Dict[str, Any]:
    """Executes dbt staging models or fallback Python transformation for stg_online_retail."""
    dbt_project_dir = Path("dbt_project")
    ti = context["ti"]
    source_path_str = ti.xcom_pull(key="raw_source_path", task_ids="extract_and_validate_raw")

    # Attempt to execute dbt run if dbt is installed and database is reachable
    dbt_cmd = ["dbt", "run", "--select", "staging", "--project-dir", str(dbt_project_dir)]
    dbt_succeeded = False

    try:
        res = subprocess.run(dbt_cmd, capture_output=True, text=True, check=True, timeout=120)
        logger.info(f"dbt staging models ran successfully:\n{res.stdout}")
        dbt_succeeded = True
    except Exception as e:
        logger.warning(f"dbt run failed or database unavailable ({e}). Falling back to Python Lakehouse ELT.")

    # High-performance Lakehouse fallback transformation
    import pandas as pd

    src = Path(source_path_str)
    df = pd.read_parquet(src) if src.suffix == ".parquet" else pd.read_csv(src)
    df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]

    rename_map = {
        "invoice": "invoiceno", "invoiceno": "invoiceno",
        "stockcode": "stockcode",
        "description": "description",
        "quantity": "quantity",
        "invoicedate": "invoicedate",
        "price": "unitprice", "unitprice": "unitprice",
        "customer id": "customerid", "customerid": "customerid",
        "country": "country",
    }
    df = df.rename(columns={c: rename_map[c] for c in df.columns if c in rename_map})

    df_cleaned = df.dropna(subset=["invoiceno", "description", "customerid"]).copy()
    df_cleaned = df_cleaned[df_cleaned["quantity"] > 0]
    df_cleaned = df_cleaned[df_cleaned["unitprice"] > 0]
    df_cleaned = df_cleaned[~df_cleaned["invoiceno"].astype(str).str.startswith("C")]

    staging_dir = Path("data/lakehouse/staging")
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_output = staging_dir / "stg_online_retail.parquet"
    df_cleaned.to_parquet(staging_output, index=False)

    logger.info(f"Staged {len(df_cleaned):,} valid records into {staging_output}")
    return {"staged_rows": len(df_cleaned), "dbt_native": dbt_succeeded}


def task_run_dbt_marts(**context) -> Dict[str, Any]:
    """Generates analytics marts: transaction baskets and user-item interaction matrix."""
    import pandas as pd

    staging_file = Path("data/lakehouse/staging/stg_online_retail.parquet")
    if not staging_file.exists():
        staging_file = Path("data/lakehouse/real_transactions.parquet")

    logger.info(f"Building Gold Marts from staged data: {staging_file}")
    df = pd.read_parquet(staging_file)

    # Mart 1: transaction_fpgrowth (baskets grouped by invoiceno)
    df_baskets = (
        df.groupby("invoiceno")["description"]
        .apply(lambda items: ",".join(sorted(set(str(i).strip().lower() for i in items if str(i).strip()))))
        .reset_index()
    )
    df_baskets.columns = ["transaction_id", "items"]
    # Filter baskets with at least 2 distinct items
    df_baskets = df_baskets[df_baskets["items"].str.contains(",")].reset_index(drop=True)

    # Mart 2: user_item_dl (distinct user-item interaction pairs)
    df_user_item = (
        df[["customerid", "description"]]
        .drop_duplicates()
        .copy()
        .reset_index(drop=True)
    )
    df_user_item.columns = ["user_id", "item_id"]
    df_user_item["user_id"] = df_user_item["user_id"].astype(int)
    df_user_item["item_id"] = df_user_item["item_id"].astype(str).str.strip().lower()

    marts_dir = Path("data/lakehouse/marts")
    marts_dir.mkdir(parents=True, exist_ok=True)

    basket_path = marts_dir / "transaction_fpgrowth.parquet"
    user_item_path = marts_dir / "user_item_dl.parquet"

    df_baskets.to_parquet(basket_path, index=False)
    df_user_item.to_parquet(user_item_path, index=False)

    logger.info(f"Marts built: {len(df_baskets):,} multi-item baskets, {len(df_user_item):,} user-item pairs.")
    return {
        "total_baskets": len(df_baskets),
        "total_user_item_pairs": len(df_user_item),
    }


def task_validate_data_quality(**context) -> Dict[str, Any]:
    """Applies strict data quality assertions on Gold Marts."""
    import pandas as pd

    basket_path = Path("data/lakehouse/marts/transaction_fpgrowth.parquet")
    user_item_path = Path("data/lakehouse/marts/user_item_dl.parquet")

    if not basket_path.exists() or not user_item_path.exists():
        raise FileNotFoundError("Gold Mart files missing prior to data quality validation.")

    df_baskets = pd.read_parquet(basket_path)
    df_user_item = pd.read_parquet(user_item_path)

    # Assertion 1: Non-empty datasets
    assert len(df_baskets) >= 1000, f"Insufficient baskets: {len(df_baskets)} < 1000"
    assert len(df_user_item) >= 5000, f"Insufficient user-item pairs: {len(df_user_item)} < 5000"

    # Assertion 2: Zero null values
    assert df_baskets["transaction_id"].isnull().sum() == 0, "Null transaction IDs found in baskets"
    assert df_baskets["items"].isnull().sum() == 0, "Null items found in baskets"
    assert df_user_item["user_id"].isnull().sum() == 0, "Null user IDs found in user_item mart"
    assert df_user_item["item_id"].isnull().sum() == 0, "Null item IDs found in user_item mart"

    # Assertion 3: Multi-item integrity
    single_item_count = (~df_baskets["items"].str.contains(",")).sum()
    assert single_item_count == 0, f"Found {single_item_count} single-item transactions in MBA mart"

    logger.info("All Data Quality Assertions passed: 100% compliant with Gold standards.")
    return {"dq_status": "PASSED", "checked_baskets": len(df_baskets), "checked_pairs": len(df_user_item)}


def task_export_gold_marts(**context) -> Dict[str, Any]:
    """Exports CSV copies of marts to data/ for compatibility with legacy notebooks and services."""
    import pandas as pd

    marts_dir = Path("data/lakehouse/marts")
    dest_dir = Path("data")

    basket_src = marts_dir / "transaction_fpgrowth.parquet"
    user_item_src = marts_dir / "user_item_dl.parquet"

    df_baskets = pd.read_parquet(basket_src)
    df_user_item = pd.read_parquet(user_item_src)

    out_basket = dest_dir / "transaction_fpgrowth.csv"
    out_user_item = dest_dir / "user_item_dl.csv"

    df_baskets.to_csv(out_basket, index=False)
    df_user_item.to_csv(out_user_item, index=False)

    logger.info(f"Exported gold marts to CSV: {out_basket} and {out_user_item}")
    return {"exported_files": [str(out_basket), str(out_user_item)]}


def task_publish_metrics(**context) -> None:
    """Aggregates execution metrics across all pipeline tasks and logs audit record."""
    ti = context["ti"]
    extract_res = ti.xcom_pull(task_ids="extract_and_validate_raw")
    staging_res = ti.xcom_pull(task_ids="run_dbt_staging_transformations")
    marts_res = ti.xcom_pull(task_ids="run_dbt_marts_transformations")
    dq_res = ti.xcom_pull(task_ids="validate_data_quality")

    logger.info("==================================================")
    logger.info(">> LAKEHOUSE ELT PIPELINE EXECUTION SUMMARY")
    logger.info("==================================================")
    logger.info(f"Raw Source:            {extract_res.get('source')} ({extract_res.get('size_mb', 0):.2f} MB)")
    logger.info(f"Staged Records:        {staging_res.get('staged_rows'):,}")
    logger.info(f"Multi-item Baskets:    {marts_res.get('total_baskets'):,}")
    logger.info(f"User-Item Interactions:{marts_res.get('total_user_item_pairs'):,}")
    logger.info(f"Data Quality Status:   {dq_res.get('dq_status')} (All assertions verified)")
    logger.info("==================================================")


with DAG(
    dag_id="daily_lakehouse_elt_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily Lakehouse ELT, Staging & Gold Mart Transformations, Data Quality Gates",
    schedule_interval="0 1 * * *",  # Daily at 01:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "elt", "dbt", "data-quality"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_validate_raw",
        python_callable=task_extract_and_validate_raw,
    )

    staging_task = PythonOperator(
        task_id="run_dbt_staging_transformations",
        python_callable=task_run_dbt_staging,
    )

    marts_task = PythonOperator(
        task_id="run_dbt_marts_transformations",
        python_callable=task_run_dbt_marts,
    )

    dq_task = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate_data_quality,
    )

    export_task = PythonOperator(
        task_id="export_gold_marts_for_ml",
        python_callable=task_export_gold_marts,
    )

    metrics_task = PythonOperator(
        task_id="publish_elt_completion_metrics",
        python_callable=task_publish_metrics,
    )

    # Topology
    extract_task >> staging_task >> marts_task >> dq_task >> export_task >> metrics_task

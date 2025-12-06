"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (
    clean_daily_transactions,
)  # pylint: disable=wrong-import-position

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Build environment variables needed by dbt commands."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """Execute a dbt command and return the completed process."""
    env = _build_env(ds_nodash)
    # Build vars JSON to pass to dbt
    dbt_vars = json.dumps({
        "clean_dir": str(CLEAN_DIR),
        "ds_nodash": ds_nodash
    })
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
            "--vars",
            dbt_vars,
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def bronze_clean_task(ds_nodash: str, **kwargs) -> None:
    """Bronze layer: Read raw CSV and clean it to parquet."""
    execution_date = datetime.strptime(ds_nodash, "%Y%m%d").date()
    output_path = clean_daily_transactions(
        execution_date=execution_date,
        raw_dir=RAW_DIR,
        clean_dir=CLEAN_DIR,
    )
    print(f"Bronze layer: Created clean parquet at {output_path}")


def silver_dbt_run_task(ds_nodash: str, **kwargs) -> None:
    """Silver layer: Run dbt models to load data into DuckDB."""
    result = _run_dbt_command("run", ds_nodash)
    print(f"dbt run stdout:\n{result.stdout}")
    if result.stderr:
        print(f"dbt run stderr:\n{result.stderr}")
    if result.returncode != 0:
        raise AirflowException(f"dbt run failed with exit code {result.returncode}")
    print("Silver layer: dbt run completed successfully")


def gold_dbt_tests_task(ds_nodash: str, **kwargs) -> None:
    """Gold layer: Run dbt tests and write quality results to JSON."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    result = _run_dbt_command("test", ds_nodash)
    
    # Determine status based on return code
    status = "passed" if result.returncode == 0 else "failed"
    
    # Write quality results to JSON file
    quality_result = {
        "ds_nodash": ds_nodash,
        "status": status,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    
    output_file = QUALITY_DIR / f"dq_results_{ds_nodash}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(quality_result, f, indent=2)
    
    print(f"Gold layer: Data quality results written to {output_file}")
    print(f"dbt test stdout:\n{result.stdout}")
    if result.stderr:
        print(f"dbt test stderr:\n{result.stderr}")
    
    # Raise exception if tests failed (after writing results)
    if result.returncode != 0:
        raise AirflowException(
            f"dbt test failed with exit code {result.returncode}. "
            f"Results saved to {output_file}"
        )


def build_dag() -> DAG:
    """Construct the medallion pipeline DAG with bronze/silver/gold tasks."""
    with DAG(
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
    ) as medallion_dag:

        bronze_clean = PythonOperator(
            task_id="bronze_clean",
            python_callable=bronze_clean_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        silver_dbt_run = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=silver_dbt_run_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        gold_dbt_tests = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=gold_dbt_tests_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        # Define task dependencies: bronze -> silver -> gold
        bronze_clean >> silver_dbt_run >> gold_dbt_tests

    return medallion_dag


dag = build_dag()

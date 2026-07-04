"""
GitHub Events — end-to-end orchestration DAG.

Hourly:
  1. produce_kafka_events  — run the local Python producer (GH Archive -> Aiven Kafka)
  2. bronze_ingest         — Databricks: drain Kafka -> Bronze Delta
  3. silver_transform      — Databricks: Bronze -> Silver (parse/clean/dedupe)
  4. gold_aggregate        — Databricks: Silver -> Gold (star schema + aggregates)

"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")

NOTEBOOK_BASE = "/Shared/github_pipeline"
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

default_args = {
    "owner": "adnan",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def _submit_run(notebook: str, params: dict | None = None) -> int:
    """
    Submit a one-time notebook run on serverless compute. Returns run_id.
    """
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json={
            "run_name": f"airflow-{notebook}",
            "queue": {"enabled": True},
            "tasks": [
                {
                    "task_key": notebook.replace("/", "_"),
                    "notebook_task": {
                        "notebook_path": f"{NOTEBOOK_BASE}/{notebook}",
                        "base_parameters": {"catalog": CATALOG, **(params or {})},
                        "source": "WORKSPACE",
                    },
                    "libraries": [],
                }
            ],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["run_id"]


def _wait(run_id: int, timeout_min: int = 60) -> None:
    """Poll a run until it terminates; raise if it didn't succeed."""
    INTERNAL_ERROR_STATES = {"INTERNAL_ERROR", "SKIPPED"}
    deadline = time.monotonic() + timeout_min * 60
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={"run_id": run_id},
            timeout=30,
        )
        resp.raise_for_status()
        state = resp.json()["state"]
        lc = state["life_cycle_state"]
        print(f"Run {run_id} state: {lc} / {state.get('result_state', 'pending')}")
        if lc == "TERMINATED":
            if state["result_state"] != "SUCCESS":
                raise RuntimeError(
                    f"Databricks run {run_id} -> {state['result_state']}: "
                    f"{state.get('state_message')}"
                )
            return
        if lc in INTERNAL_ERROR_STATES:
            raise RuntimeError(
                f"Databricks run {run_id} hit terminal state {lc}: "
                f"{state.get('state_message')}"
            )
        time.sleep(30)
    raise TimeoutError(f"Run {run_id} did not finish within {timeout_min} min")


def run_notebook(notebook: str, **context):
    params = {"run_date": context["ds"]}
    run_id = _submit_run(notebook, params)
    print(f"Submitted {notebook} as run {run_id}")
    _wait(run_id)
    print(f"{notebook} succeeded.")


with DAG(
    dag_id="github_events_pipeline",
    default_args=default_args,
    description="GH Archive -> Kafka -> Databricks medallion -> Gold",
    schedule="0 * * * *", 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["github", "kafka", "databricks", "delta"],
) as dag:

    produce = BashOperator(
        task_id="produce_kafka_events",
        bash_command=(
            "cd /opt/airflow/producer && python kafka_producer.py "
            "--date '{{ data_interval_start.strftime(\"%Y-%m-%d\") }}' "
            "--hour {{ data_interval_start.hour }}"
        ),
    )

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_notebook,
        op_kwargs={"notebook": "01_bronze_streaming"},
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_notebook,
        op_kwargs={"notebook": "02_silver_transform"},
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_notebook,
        op_kwargs={"notebook": "03_gold_aggregate"},
    )

    produce >> bronze >> silver >> gold

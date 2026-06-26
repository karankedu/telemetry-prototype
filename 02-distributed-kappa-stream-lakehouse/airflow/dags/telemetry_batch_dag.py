"""
Airflow DAG: daily batch aggregation over the Delta Lake telemetry table.

The single task (aggregate_telemetry_events) uses DockerOperator to spin up
a fresh apache/spark:3.5.1 container, bind-mount the Delta files and the
spark jobs directory from the host, and run spark-submit on
batch_aggregation.py.  Aggregation output (four df.show() tables) appears
in the Airflow task log.

Prerequisites:
  - Docker socket mounted into the Airflow container (/var/run/docker.sock)
  - HOST_PROJECT_DIR env var set to the absolute host path of the
    02-distributed-kappa-stream-lakehouse directory (set in .env)
  - apache/spark:3.5.1 image already pulled locally
  - Delta Lake files present at {HOST_PROJECT_DIR}/data/delta/telemetry_events
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")

with DAG(
    dag_id="telemetry_batch_aggregation",
    description="PySpark batch aggregation over Delta Lake telemetry events",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["telemetry", "kappa", "delta-lake", "spark"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:

    aggregate = DockerOperator(
        task_id="aggregate_telemetry_events",
        image="apache/spark:3.5.1",
        api_version="auto",
        auto_remove="success",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        user="root",
        command=(
            "/opt/spark/bin/spark-submit"
            " --packages io.delta:delta-spark_2.12:3.1.0"
            " /opt/spark-jobs/batch_aggregation.py"
        ),
        mounts=[
            # spark jobs directory — contains batch_aggregation.py
            Mount(
                source=f"{HOST_PROJECT_DIR}/spark",
                target="/opt/spark-jobs",
                type="bind",
                read_only=True,
            ),
            # Delta Lake table written by the Spark Structured Streaming job
            Mount(
                source=f"{HOST_PROJECT_DIR}/data/delta",
                target="/opt/spark-jobs/delta",
                type="bind",
                read_only=True,
            ),
            # Ivy JAR cache — persists downloaded Delta JARs across DAG runs
            Mount(
                source="ivy_cache",
                target="/root/.ivy2",
                type="volume",
            ),
        ],
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=15),
    )

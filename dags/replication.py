"""Replication DAG: Iceberg silver tables -> ClickHouse.

Runs independently from the steamspy extraction DAG. Trigger this after the
steamspy DAG completes, or manually for backfills. Snapshot-based change
detection ensures only modified partitions are reloaded.

Each table is replicated as an independent Airflow task using the same
parameterized Spark job, so failures can be retried per-table.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pipelines.common.spark.config import get_iceberg_catalog_conf, get_s3a_conf, get_spark_resource_conf

_REPLICATION_APP = "/opt/spark/jobs/replication/steamspy_replication.py"

_TABLES = [
    {
        "task_id": "replicate_steamspy_silver_stg_games",
        "iceberg_table": "iceberg.steamspy.silver_stg_games",
        "clickhouse_table": "analytics.steamspy_silver_stg_games",
    },
]


with DAG(
    dag_id="replication",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["replication", "clickhouse", "iceberg"],
    params={
        "from_snapshot": Param(
            None,
            type=["null", "integer"],
            description="Override start snapshot ID (exclusive). Leave null to use stored state.",
        ),
        "full_load": Param(
            False,
            type="boolean",
            description="Ignore state table and reload all partitions from scratch.",
        ),
    },
) as dag:

    prev = None
    for tbl in _TABLES:
        task = SparkSubmitOperator(
            task_id=tbl["task_id"],
            application=_REPLICATION_APP,
            conn_id="spark_default",
            conf={
                **get_s3a_conf(),
                **get_spark_resource_conf(),
                **get_iceberg_catalog_conf(),
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.replication.iceberg_table": tbl["iceberg_table"],
                "spark.replication.clickhouse_table": tbl["clickhouse_table"],
                "spark.replication.from_snapshot": "{{ params.from_snapshot or '' }}",
                "spark.replication.full_load": "{{ params.full_load | lower }}",
            },
        )
        if prev is not None:
            prev >> task
        prev = task
